#![deny(warnings)]

use std::collections::{HashMap, HashSet};
use std::io::{stdin, BufRead, Lines, StdinLock};
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use fclones::config::GroupConfig;
use fclones::log::StdLog;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use indicatif::HumanBytes;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

use dedupetool::diskblade::{DiskBladeConfig, FileOffset, FileSectionTarget};
use dedupetool::ioctl_fideduperange::{dedupe_files, DedupeRequest, DedupeResponse};
use dedupetool::ioctl_fiemap::get_extents;
use dedupetool::termhelp::{log_diag, StderrStyle};

type DedupeResult = Result<Option<DedupeInfo>, DedupeError>;

/// File section de-duplicator.
#[derive(Parser)]
#[clap(name = "dedupetool", version)]
struct DedupeTool {
    /// Maximum concurrent de-dupe calls.
    #[clap(short, long, default_value = "32")]
    max_concurrency: usize,
    /// Should the up-front FIEMAP check for already shared sections be skipped?
    /// This trades size report accuracy for speed.
    #[clap(long)]
    skip_fiemap: bool,
    /// True to run without making changes, and print the target information.
    #[clap(short = 'n', long)]
    dry_run: bool,
    /// Indicates how to find the targets to de-dupe.
    #[clap(subcommand)]
    subcommand: DeduplicationTargetFinder,
}

#[derive(Subcommand)]
enum DeduplicationTargetFinder {
    /// Find file *sections* using a specialized algorithm based on FastCDC.
    /// This enables more fine-grained de-duplication than the other modes.
    DiskBlade(DiskBladeConfig),
    /// Load files from stdin.
    Stdin,
    /// Find files using `fclones`. Takes the same arguments as `fclones group`.
    Fclones(GroupConfig),
}

impl DeduplicationTargetFinder {
    async fn into_target_iter(self) -> Box<dyn Iterator<Item = DeduplicationTarget>> {
        match self {
            DeduplicationTargetFinder::DiskBlade(config) => {
                Box::new(diskblade_targets(config).await)
            }
            DeduplicationTargetFinder::Stdin => Box::new(stdin_fdupes_targets()),
            DeduplicationTargetFinder::Fclones(config) => Box::new(fclones_targets(config)),
        }
    }
}

impl Default for DeduplicationTargetFinder {
    fn default() -> Self {
        Self::Stdin
    }
}

#[tokio::main]
async fn main() {
    let args: DedupeTool = DedupeTool::parse();

    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let concurrency_mutex = Arc::new(Semaphore::new(args.max_concurrency));
    let mut dedupe_futures = FuturesUnordered::new();

    for target in args.subcommand.into_target_iter().await {
        if args.dry_run {
            match target {
                DeduplicationTarget::Files(files) => {
                    for file in files {
                        log_diag(file.display().to_string().success_style());
                    }
                }
                DeduplicationTarget::Sections(sections) => {
                    log_diag(format!("{:?}", sections).success_style());
                }
            }
            continue;
        }

        let skip_fiemap = args.skip_fiemap;
        let tracker = tracker.clone();
        let concurrency_mutex = concurrency_mutex.clone();
        // Avoid over-pulling from the iterator by waiting for the semaphore to be available.
        let owned = concurrency_mutex.acquire_owned().await.unwrap();
        dedupe_futures.push(tokio::spawn(async move {
            let _permit = owned;
            let result = process_dedupe(skip_fiemap, target).await;
            let mut tracker = tracker.lock().await;
            tracker.record_result(result);
        }));
    }

    while let Some(f) = dedupe_futures.next().await {
        f.expect("Panic in dedupe future");
    }

    let tracker = tracker.lock().await;

    log_diag(format!("Saved up to {} total!", HumanBytes(tracker.max_bytes_saved)).success_style());

    if tracker.any_failed {
        exit(1);
    }
}

#[derive(Debug, Clone)]
enum DeduplicationTarget {
    Files(Vec<PathBuf>),
    Sections(FileSectionTarget),
}

async fn diskblade_targets(config: DiskBladeConfig) -> impl Iterator<Item = DeduplicationTarget> {
    dedupetool::diskblade::group_files(config)
        .await
        .expect("Failed to group files")
        .into_iter()
        .map(DeduplicationTarget::Sections)
}

fn fclones_targets(config: GroupConfig) -> impl Iterator<Item = DeduplicationTarget> {
    fclones::group_files(&config, &StdLog::new())
        .expect("Failed to group files")
        .into_iter()
        .map(|g| {
            DeduplicationTarget::Files(g.files.into_iter().map(|f| f.path.to_path_buf()).collect())
        })
}

fn stdin_fdupes_targets() -> impl Iterator<Item = DeduplicationTarget> {
    struct Iter {
        iter: Lines<StdinLock<'static>>,
        dedup_lines: Vec<String>,
    }

    impl Iterator for Iter {
        type Item = Vec<PathBuf>;

        fn next(&mut self) -> Option<Self::Item> {
            for line_res in self.iter.by_ref() {
                let line = match line_res {
                    Ok(l) => l.trim_end().to_owned(),
                    Err(e) => panic!("Failed to read from stdin: {}", e),
                };
                if line.is_empty() {
                    if self.dedup_lines.len() > 1 {
                        return Some(self.dedup_lines.drain(..).map(PathBuf::from).collect());
                    }
                    continue;
                }
                self.dedup_lines.push(line);
            }
            (self.dedup_lines.len() > 1)
                .then(|| self.dedup_lines.drain(..).map(PathBuf::from).collect())
        }
    }

    Iter {
        iter: stdin().lock().lines(),
        dedup_lines: Vec::new(),
    }
    .map(DeduplicationTarget::Files)
}

async fn process_dedupe(skip_fiemap: bool, target: DeduplicationTarget) -> DedupeResult {
    internal_process_dedupe(skip_fiemap, target.clone())
        .await
        .map_err(|e| DedupeError { target, source: e })
}

async fn internal_process_dedupe(
    skip_fiemap: bool,
    target: DeduplicationTarget,
) -> Result<Option<DedupeInfo>, std::io::Error> {
    // Reduce target to FileSectionTarget only.
    let mut target = match target {
        DeduplicationTarget::Files(files) => resolve_file_sections(files).await?,
        DeduplicationTarget::Sections(t) => t,
    };
    if !skip_fiemap {
        remove_already_shared_file_sections(&mut target).await?;
    }

    if target.offsets.len() < 2 {
        // There are no files to deduplicate.
        return Ok(None);
    }
    let (first, rest) = target.offsets.split_first().unwrap();

    let first_file = tokio::fs::File::open(&first.file()).await?.into_std().await;

    // 'static-ify first & rest by cloning them
    let src_range = first.offset()..(first.offset() + target.length);
    let rest = Vec::from(rest);
    let responses = tokio::task::spawn_blocking(move || {
        let dest_reqs = rest
            .into_iter()
            .map(|file| {
                let request = DedupeRequest::new(file.file(), file.offset());
                Ok((file, request))
            })
            .collect::<Result<HashMap<FileOffset, DedupeRequest>, std::io::Error>>()?;
        dedupe_files(&first_file, src_range, dest_reqs)
    })
    .await
    .expect("failed to spawn blocking")?;

    let mut offsets_errored = HashMap::<FileOffset, std::io::Error>::new();
    let mut offsets_affected = HashSet::<FileOffset>::new();
    let mut total_bytes_saved = 0;

    for (file, response_vec) in responses {
        for response in response_vec {
            match response {
                DedupeResponse::RangeSame { bytes_deduped } => {
                    if bytes_deduped > 0 {
                        offsets_affected.insert(file.clone());
                        total_bytes_saved += bytes_deduped;
                    }
                }
                DedupeResponse::Error(e) => {
                    offsets_errored.insert(file.clone(), e);
                }
                DedupeResponse::RangeDiffers => {
                    // does nothing, we don't care if this occurred
                }
            }
        }
    }

    Ok(Some(DedupeInfo {
        size: target.length,
        offset_targeted: first.clone(),
        offsets_errored,
        offsets_affected: offsets_affected.into_iter().collect(),
        total_bytes_saved,
    }))
}

async fn resolve_file_sections(files: Vec<PathBuf>) -> Result<FileSectionTarget, std::io::Error> {
    if files.is_empty() {
        return Ok(FileSectionTarget {
            length: 0,
            offsets: Vec::new(),
        });
    }
    // Make an assumption that all files are the same size
    let size = tokio::fs::metadata(&files[0]).await?.len();

    let offsets = files
        .into_iter()
        .map(|file| FileOffset::new(file, 0))
        .collect();
    Ok(FileSectionTarget {
        length: size,
        offsets,
    })
}

async fn remove_already_shared_file_sections(
    target: &mut FileSectionTarget,
) -> Result<(), std::io::Error> {
    let size = target.length;
    // Map of Vec<(offset, len)> to Vec of offsets
    let mut physical_extent_buckets = HashMap::<Vec<(u64, u64)>, Vec<FileOffset>>::new();
    for section in &target.offsets {
        let offset = section.offset();
        let f = tokio::fs::File::open(&section.file())
            .await?
            .into_std()
            .await;
        let extents =
            tokio::task::spawn_blocking(move || get_extents(&f, offset..(offset + size), false))
                .await
                .expect("failed to spawn blocking")?;
        physical_extent_buckets
            .entry(
                extents
                    .into_iter()
                    .map(|ext| (ext.logical_offset, ext.length))
                    .collect(),
            )
            .or_default()
            .push(section.clone());
    }

    let biggest_vec = physical_extent_buckets
        .values()
        .max_by_key(|v| v.len())
        .unwrap();

    if biggest_vec.len() == 1 {
        // There are no shared groups, existing vec is good
    } else if biggest_vec.len() == target.offsets.len() {
        // Everything is shared! Empty the offsets list!
        target.offsets.clear();
    } else {
        // Some offsets are shared, take the biggest vec and remove all but 1 of them from the offsets
        let (_, rest) = biggest_vec.split_first().unwrap();
        let remove_these: HashSet<_> = rest.iter().collect();
        target.offsets.retain(|x| !remove_these.contains(x));
    }

    Ok(())
}

#[derive(Default)]
struct Tracker {
    max_bytes_saved: u64,
    any_failed: bool,
}

impl Tracker {
    fn record_result(&mut self, result: DedupeResult) {
        match result {
            Ok(Some(ref dedupe)) => {
                self.max_bytes_saved += dedupe.total_bytes_saved;
            }
            Ok(_) => {}
            Err(_) => {
                self.any_failed = true;
            }
        };
        print_task_completion(result);
    }
}

fn print_task_completion(result: DedupeResult) {
    match result {
        Ok(Some(dedupe)) => {
            eprintln!(
                "==> De-dupe Targeting {} [{}-{}]",
                dedupe.offset_targeted.file().display(),
                dedupe.offset_targeted.offset(),
                dedupe.offset_targeted.offset() + dedupe.size,
            );
            if !dedupe.offsets_affected.is_empty() {
                eprintln!(
                    "Saved {} by re-using content in:",
                    HumanBytes(dedupe.total_bytes_saved),
                );
                for affected in dedupe.offsets_affected {
                    eprintln!("    {}", affected.file().display());
                }
            }
            if !dedupe.offsets_errored.is_empty() {
                log_diag("Errors encountered during the above operation:".error_style());
                for (section, error) in dedupe.offsets_errored {
                    log_diag(format!("    {}: {}", section.file().display(), error).error_style());
                }
            }
        }
        Ok(_) => {}
        Err(e) => {
            log_diag(format!("Got {} while trying to dedupe these files:", e.source).error_style());
            let files = match e.target {
                DeduplicationTarget::Files(files) => files,
                DeduplicationTarget::Sections(target) => {
                    target.offsets.into_iter().map(|s| s.into_file()).collect()
                }
            };
            for targeted in files {
                log_diag(format!("    {}", targeted.display()).error_style());
            }
        }
    }
}

#[derive(Error, Debug)]
#[error("Error while de-duplicating {target:?}: {source}")]
struct DedupeError {
    target: DeduplicationTarget,
    source: std::io::Error,
}

#[derive(Debug)]
struct DedupeInfo {
    size: u64,
    offset_targeted: FileOffset,
    offsets_errored: HashMap<FileOffset, std::io::Error>,
    offsets_affected: Vec<FileOffset>,
    total_bytes_saved: u64,
}

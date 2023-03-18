#![deny(warnings)]

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{stdin, BufRead, Lines, StdinLock};
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

use clap::Parser;
use console::Style;
use fclones::config::GroupConfig;
use fclones::log::StdLog;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use size_format::SizeFormatterBinary;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

use dedupetool::ioctl_fideduperange::{dedupe_files, DedupeRequest, DedupeResponse};
use dedupetool::ioctl_fiemap::get_extents;

fn success_style() -> Style {
    Style::new().for_stderr().green()
}

fn error_style() -> Style {
    Style::new().for_stderr().red()
}

type DedupeResult = Result<Option<DedupeInfo>, DedupeError>;

/// File de-duplicator.
#[derive(Parser)]
#[clap(name = "dedupetool", version)]
struct DedupeTool {
    /// Maximum concurrent de-dupe calls.
    #[clap(short, long, default_value = "32")]
    max_concurrency: usize,
    /// Should the up-front FIEMAP check for already shared files be skipped?
    /// This trades size report accuracy for speed.
    #[clap(long)]
    skip_fiemap: bool,
    /// If present, indicates how to load the files to de-dupe.
    #[clap(subcommand)]
    subcommand: Option<FileLoadMode>,
}

#[derive(Parser)]
enum FileLoadMode {
    /// Load files from stdin.
    Stdin,
    /// Find files using `fclones`. Takes the same arguments as `fclones group`.
    Fclones(GroupConfig),
}

impl Default for FileLoadMode {
    fn default() -> Self {
        Self::Stdin
    }
}

#[tokio::main]
async fn main() {
    let args: DedupeTool = DedupeTool::parse();

    let to_dedupe: Box<dyn Iterator<Item = Vec<PathBuf>>> =
        if let Some(FileLoadMode::Fclones(fclones_args)) = args.subcommand {
            Box::new(fclones_file_groups(fclones_args))
        } else {
            Box::new(stdin_fdupes_file_groups())
        };

    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let concurrency_mutex = Arc::new(Semaphore::new(args.max_concurrency));
    let mut dedupe_futures = FuturesUnordered::new();

    for files in to_dedupe {
        let skip_fiemap = args.skip_fiemap;
        let files = files
            .into_iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        let tracker = tracker.clone();
        let concurrency_mutex = concurrency_mutex.clone();
        // Avoid over-pulling from the iterator by waiting for the semaphore to be available.
        let owned = concurrency_mutex.acquire_owned().await.unwrap();
        dedupe_futures.push(tokio::spawn(async move {
            let _permit = owned;
            let result = process_dedupe(skip_fiemap, files).await;
            let mut tracker = tracker.lock().await;
            tracker.record_result(result);
        }));
    }

    while let Some(f) = dedupe_futures.next().await {
        f.expect("Panic in dedupe future");
    }

    let tracker = tracker.lock().await;

    eprintln!(
        "{}",
        success_style().apply_to(format!(
            "Saved up to {}B total!",
            SizeFormatterBinary::new(tracker.max_bytes_saved)
        ))
    );

    if tracker.any_failed {
        exit(1);
    }
}

fn fclones_file_groups(config: GroupConfig) -> impl Iterator<Item = Vec<PathBuf>> {
    fclones::group_files(&config, &StdLog::new())
        .expect("Failed to group files")
        .into_iter()
        .map(|g| g.files.into_iter().map(|f| f.path.to_path_buf()).collect())
}

fn stdin_fdupes_file_groups() -> impl Iterator<Item = Vec<PathBuf>> {
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
}

async fn process_dedupe(skip_fiemap: bool, files: Vec<String>) -> DedupeResult {
    internal_process_dedupe(skip_fiemap, Cow::Borrowed(&files))
        .await
        .map_err(|e| DedupeError {
            target_files: files,
            source: e,
        })
}

async fn internal_process_dedupe(
    skip_fiemap: bool,
    mut files: Cow<'_, [String]>,
) -> Result<Option<DedupeInfo>, std::io::Error> {
    if !skip_fiemap {
        remove_already_shared_files(files.to_mut()).await?;
    }

    if files.len() < 2 {
        // There are no files to deduplicate.
        return Ok(None);
    }
    let (first, rest) = files.split_first().unwrap();

    if tokio::fs::metadata(first).await?.len() < 16 * 1024 {
        // Too small for it to be worth.
        return Ok(None);
    }

    let first_file = tokio::fs::File::open(first).await?.into_std().await;

    // 'static-ify first & rest by cloning them
    let first_static = first.clone();
    let rest = Vec::from(rest);
    let responses = tokio::task::spawn_blocking(move || {
        let dest_reqs = rest
            .iter()
            .map(|file| Ok((file.clone(), DedupeRequest::new(file, 0))))
            .collect::<Result<HashMap<String, DedupeRequest>, std::io::Error>>()?;
        dedupe_files(
            &first_file,
            0..std::fs::metadata(first_static)?.len(),
            dest_reqs,
        )
    })
    .await
    .expect("failed to spawn blocking")?;

    let mut files_errored = HashMap::<String, std::io::Error>::new();
    let mut files_affected = HashSet::<String>::new();
    let mut total_bytes_saved = 0;

    for (file, response_vec) in responses {
        for response in response_vec {
            match response {
                DedupeResponse::RangeSame { bytes_deduped } => {
                    if bytes_deduped > 0 {
                        files_affected.insert(file.clone());
                        total_bytes_saved += bytes_deduped;
                    }
                }
                DedupeResponse::Error(e) => {
                    files_errored.insert(file.clone(), e);
                }
                DedupeResponse::RangeDiffers => {
                    // does nothing, we don't care if this occurred
                }
            }
        }
    }

    Ok(Some(DedupeInfo {
        file_targeted: first.clone(),
        files_errored,
        files_affected: files_affected.into_iter().collect(),
        total_bytes_saved,
    }))
}

async fn remove_already_shared_files(files: &mut Vec<String>) -> Result<(), std::io::Error> {
    // Map of Vec<(offset, len)> to Vec of files
    let mut physical_extent_buckets = HashMap::<Vec<(u64, u64)>, Vec<String>>::new();
    for file in files.iter() {
        let f = tokio::fs::File::open(file).await?.into_std().await;
        let extents = tokio::task::spawn_blocking(move || get_extents(&f, 0..u64::MAX, false))
            .await
            .expect("failed to spawn blocking")?;
        physical_extent_buckets
            .entry(
                extents
                    .into_iter()
                    .map(|ext| (ext.physical_offset, ext.length))
                    .collect(),
            )
            .or_insert_with(Vec::new)
            .push(file.clone());
    }

    let biggest_vec = physical_extent_buckets
        .values()
        .max_by_key(|v| v.len())
        .unwrap();

    if biggest_vec.len() == 1 {
        // There are no shared groups, existing vec is good
    } else if biggest_vec.len() == files.len() {
        // Everything is shared! Empty the files list!
        files.clear();
    } else {
        // Some files are shared, take the biggest vec and remove all but 1 of them from the files
        let (_, rest) = biggest_vec.split_first().unwrap();
        let remove_these: HashSet<_> = rest.iter().collect();
        files.retain(|x| !remove_these.contains(x));
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
            eprintln!("==> De-dupe Targeting {}", dedupe.file_targeted);
            if !dedupe.files_affected.is_empty() {
                eprintln!(
                    "Saved {}B by re-using content in:",
                    SizeFormatterBinary::new(dedupe.total_bytes_saved),
                );
                for affected in dedupe.files_affected {
                    eprintln!("    {}", affected);
                }
            }
            if !dedupe.files_errored.is_empty() {
                eprintln!(
                    "{}",
                    error_style().apply_to("Errors encountered during the above operation:")
                );
                for (file, error) in dedupe.files_errored {
                    eprintln!(
                        "{}",
                        error_style().apply_to(format!("    {}: {}", file, error))
                    );
                }
            }
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "{}",
                error_style().apply_to(format!(
                    "Got {} while trying to dedupe these files:",
                    e.source
                ))
            );
            for targeted in e.target_files {
                eprintln!("{}", error_style().apply_to(format!("    {}", targeted)));
            }
        }
    }
}

#[derive(Error, Debug)]
#[error("Error while de-duplicating {target_files:?}: {source}")]
struct DedupeError {
    target_files: Vec<String>,
    source: std::io::Error,
}

#[derive(Debug)]
struct DedupeInfo {
    file_targeted: String,
    files_errored: HashMap<String, std::io::Error>,
    files_affected: Vec<String>,
    total_bytes_saved: u64,
}

use std::collections::HashSet;
use std::ops::Deref;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use fastcdc::v2020::{AsyncStreamCDC, Normalization};
use futures::io::Empty;
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar};
use thiserror::Error;
use tokio::sync::Mutex;
use walkdir::{DirEntry, Error};

use crate::diskblade::chunk_manager::{Chunk, ChunkManager};
use crate::termhelp::{log_diag, DedupetoolProgressBar, StderrStyle};
use crate::tokio_futures_io::TokioFuturesIo;

mod chunk_manager;
mod tea_merger;

/// A target to deduplicate.
#[derive(Debug, Clone)]
pub struct FileSectionTarget {
    /// The length to deduplicate after each offset in [`offsets`].
    pub length: u64,
    /// The file offsets to de-dupe at.
    pub offsets: Vec<FileOffset>,
}

/// An offset into a file.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FileOffset {
    /// The file this is an offset into.
    file: PathBuf,
    /// The offset into the file of the section.
    offset: u64,
}

impl FileOffset {
    pub fn new(file: PathBuf, offset: u64) -> Self {
        // normalize file
        let file = file.canonicalize().expect("file should exist");
        Self { file, offset }
    }

    pub fn file(&self) -> &PathBuf {
        &self.file
    }

    pub fn into_file(self) -> PathBuf {
        self.file
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[derive(Error, Debug)]
pub enum DiskBladeError {
    #[error("Failed to load chunks of files: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Args)]
pub struct DiskBladeConfig {
    /// The directory to process.
    pub directory: PathBuf,
    /// The minimum size of a section to deduplicate.
    /// Defaults to 64K, as a trade-off between deduplication and potential fragmentation.
    #[clap(
    long,
    default_value = "65536",
    value_parser = clap::value_parser!(u32).range(
        fastcdc::v2020::MINIMUM_MIN as i64..=fastcdc::v2020::MINIMUM_MAX as i64
    )
    )]
    pub min_size: u32,
    /// The maximum size of a section to deduplicate.
    /// Defaults to no maximum.
    #[clap(
    long,
    value_parser = clap::value_parser!(u32).range(
        fastcdc::v2020::MAXIMUM_MIN as i64..=fastcdc::v2020::MAXIMUM_MAX as i64
    )
    )]
    pub max_size: Option<u32>,
    /// The number of threads to chunk with.
    /// Defaults to the number of logical cores * 2.
    #[clap(long)]
    pub threads: Option<usize>,
}

/// Run the DiskBlade algorithm for deduplication.
///
/// # Returns
/// A list of targets to deduplicate.
pub async fn group_files(
    config: DiskBladeConfig,
) -> Result<Vec<FileSectionTarget>, DiskBladeError> {
    if let Some(max_size) = config.max_size {
        assert!(max_size >= config.min_size);
    }
    // 1. Walk the directory
    // 2. Concurrently, chunk each file using fastcdc
    // 3. Chunks of the same size and hash are checked for equality, then grouped.
    // 4. Where possible, chunks are grouped into a single target.
    let multi_progress = MultiProgress::new();
    let walking_progress = multi_progress.add(
        ProgressBar::dedupetool_spinner("file(s)")
            .with_message(format!(
                "Walking `{}`",
                config.directory.display().style().magenta()
            ))
            .with_steady_tick_dedupetool(),
    );
    let chunking_progress = multi_progress.add(
        ProgressBar::dedupetool_spinner("file(s)")
            .with_message("Chunking files...")
            .with_steady_tick_dedupetool(),
    );
    let (walk_send, walk_recv) = flume::bounded(10_000);
    let (chunk_send, chunk_recv) = flume::bounded(10_000);
    let chunking_for_walking_progress = chunking_progress.clone();
    let directory = config.directory.clone();
    let walking_task = tokio::task::spawn(async move {
        let mut walker = walkdir::WalkDir::new(&directory).into_iter();
        while let Some(entry) = tokio::task::block_in_place(|| walker.next()) {
            walking_progress.inc(1);
            walk_send
                .send_async(entry)
                .await
                .expect("walk send should succeed");
        }
        // Now that we know how many entries there are, set the length of the progress bar.
        chunking_for_walking_progress.set_length(walking_progress.position());
        chunking_for_walking_progress.set_style_dedupetool();
        chunking_for_walking_progress.enable_steady_tick_dedupetool();
        walking_progress.finish_with_message(format!(
            "Finished walking `{}`",
            directory.display().style().magenta()
        ));
    });
    let seen_inodes = Arc::new(Mutex::new(HashSet::new()));
    let threads = config.threads.unwrap_or_else(|| num_cpus::get() * 2);
    let chunking_tasks = (0..threads)
        .map(|_| {
            let walk_recv = walk_recv.clone();
            let chunk_send = chunk_send.clone();
            let directory = config.directory.clone();
            let seen_inodes = Arc::clone(&seen_inodes);

            const DEFAULT_MAX_CHUNK_SIZE: u32 = fastcdc::v2020::MAXIMUM_MAX;
            /// Default average chunk size to 128K.
            const DEFAULT_AVG_CHUNK_SIZE: u32 = 128 * 1024;
            let min = config.min_size;
            let max = config.max_size.unwrap_or(DEFAULT_MAX_CHUNK_SIZE);
            let average = if (min..=max).contains(&DEFAULT_AVG_CHUNK_SIZE) {
                // If it fits, use our preferred average chunk size.
                DEFAULT_AVG_CHUNK_SIZE
            } else {
                // Generate average chunk size between min and max, bounded by fastcdc's limits.
                ((min + max) / 2).clamp(fastcdc::v2020::AVERAGE_MIN, fastcdc::v2020::AVERAGE_MAX)
            };
            let mut chunker = Some(AsyncStreamCDC::with_level(
                futures::io::empty(),
                min,
                average,
                max,
                Normalization::Level1,
            ));

            let chunking_progress = chunking_progress.clone();
            tokio::spawn(async move {
                let seen_inodes = seen_inodes.deref();
                while let Ok(entry) = walk_recv.recv_async().await {
                    let result =
                        process_entry(seen_inodes, &mut chunker, &directory, min, entry).await;
                    chunking_progress.inc(1);
                    let data = match result {
                        Ok(Some(data)) => data,
                        Ok(None) => continue,
                        Err(err) => {
                            log_diag(err.error_style());
                            continue;
                        }
                    };
                    chunk_send
                        .send_async(data)
                        .await
                        .expect("chunk send should succeed");
                }
                chunking_progress.finish_with_message("Finished chunking files.");
            })
        })
        .collect::<Vec<_>>();
    // Explicitly drop, since other senders have been moved into tasks.
    drop(chunk_send);
    drop(chunking_progress);

    let mut stream = chunk_recv.into_stream();
    let mut chunk_manager = ChunkManager::default();

    while let Some((path, chunks)) = StreamExt::next(&mut stream).await {
        chunk_manager.push_path(path, chunks);
    }

    walking_task.await.expect("walking task should succeed");
    for task in chunking_tasks {
        task.await.expect("chunking task should succeed");
    }
    drop(multi_progress);

    eprintln!("Converting into targets...");

    Ok(chunk_manager.into_file_section_targets())
}

#[derive(Error, Debug)]
pub enum DiskBladeInternalError {
    #[error("Error walking directory {directory}: {error}")]
    WalkDirError {
        directory: PathBuf,
        #[source]
        error: Error,
    },
    #[error("Error getting metadata for {file}: {error}")]
    MetadataError {
        file: PathBuf,
        #[source]
        error: walkdir::Error,
    },
    #[error("Error chunking file {file}: {error}")]
    ChunkingError {
        file: PathBuf,
        #[source]
        error: fastcdc::v2020::Error,
    },
}

async fn process_entry(
    seen_inodes: &Mutex<HashSet<u64>>,
    chunker: &mut Option<AsyncStreamCDC<Empty>>,
    directory: &Path,
    min_size: u32,
    entry: Result<DirEntry, walkdir::Error>,
) -> Result<Option<(PathBuf, Vec<Chunk>)>, DiskBladeInternalError> {
    let entry = entry.map_err(|err| DiskBladeInternalError::WalkDirError {
        directory: directory.to_owned(),
        error: err,
    })?;
    if !entry.file_type().is_file() {
        return Ok(None);
    }
    let metadata = entry
        .metadata()
        .map_err(|err| DiskBladeInternalError::MetadataError {
            file: entry.path().to_owned(),
            error: err,
        })?;
    if metadata.len() < u64::from(min_size) {
        return Ok(None);
    }
    if metadata.permissions().readonly() {
        return Ok(None);
    }
    {
        let mut guard = seen_inodes.lock().await;
        if !guard.insert(metadata.ino()) {
            return Ok(None);
        }
    }
    let chunks = chunk_file(chunker, entry.path(), min_size).await?;
    Ok(Some((entry.path().to_owned(), chunks)))
}

async fn chunk_file(
    chunker: &mut Option<AsyncStreamCDC<Empty>>,
    file: &Path,
    min: u32,
) -> Result<Vec<Chunk>, DiskBladeInternalError> {
    let f = TokioFuturesIo::new(tokio::fs::File::open(file).await.map_err(|err| {
        DiskBladeInternalError::ChunkingError {
            file: file.to_owned(),
            error: err.into(),
        }
    })?)
    .await;
    let owned_chunker = chunker.take().expect("chunker should exist");
    let mut stream_cdc = owned_chunker.reuse(f);
    let mut iter = Box::pin(stream_cdc.as_stream());
    let mut chunks = Vec::new();
    while let Some(chunk) = iter.next().await {
        let chunk = chunk.map_err(|err| DiskBladeInternalError::ChunkingError {
            file: file.to_owned(),
            error: err,
        })?;
        if chunk.length < min as usize {
            // Last chunk may be smaller than min, resulting in no hash. Ignore it.
            continue;
        }
        chunks.push(Chunk {
            hash: chunk.hash,
            offset: chunk.offset,
            // guaranteed because the maximum length is a u32
            length: chunk.length as u32,
        });
    }
    drop(iter);

    *chunker = Some(stream_cdc.reuse(futures::io::empty()));

    Ok(chunks)
}

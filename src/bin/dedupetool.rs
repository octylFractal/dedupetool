#![deny(warnings)]

use std::collections::{HashMap, HashSet};
use std::io::{stdin, BufRead};
use std::process::exit;
use std::sync::Arc;

use console::Style;
use size_format::SizeFormatterBinary;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

use structopt::StructOpt;

use dedupetool::ioctl_fideduperange::{dedupe_files, DedupeRequest, DedupeResponse};
use dedupetool::ioctl_fiemap::get_extents;

fn success_style() -> Style {
    Style::new().for_stderr().green()
}

fn error_style() -> Style {
    Style::new().for_stderr().red()
}

type DedupeResult = Result<Option<DedupeInfo>, DedupeError>;

#[derive(StructOpt)]
#[structopt(name = "dedupetool", about = "File de-deuplicator")]
struct DedupeTool {
    /// Maximum concurrent de-dupe calls.
    #[structopt(short, long, default_value = "32")]
    max_concurrency: usize,
}

#[tokio::main]
async fn main() {
    let args: DedupeTool = DedupeTool::from_args();
    let permits = args.max_concurrency;
    let semaphore = Arc::new(Semaphore::new(permits));
    let (push_result, mut read_result) = tokio::sync::mpsc::channel::<DedupeResult>(32);

    let printer_task = tokio::task::spawn(async move {
        let mut max_bytes_saved: u64 = 0;
        let mut any_failed = false;
        while let Some(result) = read_result.recv().await {
            match result {
                Ok(Some(ref dedupe)) => {
                    max_bytes_saved += dedupe.total_bytes_saved;
                }
                Ok(_) => {}
                Err(_) => {
                    any_failed = true;
                }
            };
            print_task_completion(result);
        }
        (max_bytes_saved, any_failed)
    });

    let mut dedup_lines = Vec::<String>::new();
    let do_kick_off = |files| kick_off(files, Arc::clone(&semaphore), push_result.clone());
    for line_res in stdin().lock().lines() {
        let line = match line_res {
            Ok(l) => l.trim_end().to_owned(),
            Err(e) => panic!("Failed to read from stdin: {}", e),
        };
        if line.is_empty() {
            if dedup_lines.len() > 1 {
                do_kick_off(dedup_lines.clone());
            }
            dedup_lines.clear();
            continue;
        }
        dedup_lines.push(line);
    }

    if !dedup_lines.is_empty() {
        do_kick_off(dedup_lines);
    }

    // drop our sender ref, so that when all tasks finish, the receiver closes
    drop(push_result);

    // await the end of printing, which is also after all tasks finish (due to above drop)
    let (max_bytes_saved, any_failed) = printer_task.await.unwrap();

    eprintln!(
        "{}",
        success_style().apply_to(format!(
            "Saved up to {}B total!",
            SizeFormatterBinary::new(max_bytes_saved)
        ))
    );

    if any_failed {
        exit(1);
    }
}

fn kick_off(files: Vec<String>, semaphore: Arc<Semaphore>, push_result: Sender<DedupeResult>) {
    tokio::task::spawn(async move {
        let _permit = semaphore.acquire().await.expect("Failed to get permit");

        let result = process_dedupe(files.clone())
            .await
            .map_err(|e| DedupeError {
                target_files: files,
                source: e,
            });

        push_result.send(result).await.expect("Send failed");
    });
}

async fn process_dedupe(mut files: Vec<String>) -> Result<Option<DedupeInfo>, std::io::Error> {
    remove_already_shared_files(&mut files).await?;
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

    let responses: HashMap<String, Vec<DedupeResponse>> = tokio::task::block_in_place(move || {
        let dest_reqs = rest
            .iter()
            .map(|file| {
                Ok((
                    file.clone(),
                    DedupeRequest::new(std::fs::OpenOptions::new().write(true).open(file)?, 0),
                ))
            })
            .collect::<Result<HashMap<String, DedupeRequest>, std::io::Error>>()?;
        dedupe_files(&first_file, 0..std::fs::metadata(first)?.len(), dest_reqs)
    })?;

    let mut files_errored = HashMap::<String, std::io::Error>::new();
    let mut files_affected = HashSet::<String>::new();
    let mut total_bytes_saved = 0;

    for (file, response_vec) in responses {
        for response in response_vec {
            match response {
                DedupeResponse::RangeSame { bytes_deduped } => {
                    files_affected.insert(file.clone());
                    total_bytes_saved += bytes_deduped;
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
        let extents = tokio::task::block_in_place(|| get_extents(&f, 0..u64::MAX, false))?;
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

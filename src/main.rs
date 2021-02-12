#![deny(warnings)]

use std::collections::HashMap;
use std::io::{stdin, BufRead};
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use console::Style;
use size_format::SizeFormatterBinary;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

use crate::ioctl_fideduperange::{dedupe_files, DedupeRequest, DedupeResponse};

mod ioctl_fideduperange;

fn error_style() -> Style {
    return Style::new().for_stderr().red();
}

#[tokio::main]
async fn main() {
    // up to 64 ioctls at a time
    let permits = 64;
    let semaphore = Arc::new(Semaphore::new(permits));
    let any_failed = Arc::new(AtomicBool::new(false));
    let (push_result, mut read_result) =
        tokio::sync::mpsc::channel::<Result<DedupeResult, DedupeError>>(32);

    let printer_any_failed = Arc::clone(&any_failed);
    let printer_task = tokio::task::spawn(async move {
        while let Some(result) = read_result.recv().await {
            if result.is_err() {
                printer_any_failed.store(true, Ordering::Relaxed);
            }
            print_task_completion(result);
        }
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
    printer_task.await.unwrap();

    if any_failed.load(Ordering::Relaxed) {
        exit(1);
    }
}

fn kick_off(
    files: Vec<String>,
    semaphore: Arc<Semaphore>,
    push_result: Sender<Result<DedupeResult, DedupeError>>,
) {
    tokio::task::spawn(async move {
        let _permit = semaphore.acquire().await.expect("Failed to get permit");

        let result = process_dedupe(files.clone()).map_err(|e| DedupeError {
            target_files: files,
            source: e,
        });

        push_result.send(result).await.expect("Send failed");
    });
}

fn process_dedupe(files: Vec<String>) -> Result<DedupeResult, std::io::Error> {
    let (first, rest) = files.split_first().unwrap();

    let first_file = std::fs::File::open(first)?;
    let dest_reqs = rest
        .into_iter()
        .map(|file| {
            Ok((
                file.clone(),
                DedupeRequest::new(std::fs::OpenOptions::new().write(true).open(file)?, 0),
            ))
        })
        .collect::<Result<HashMap<String, DedupeRequest>, std::io::Error>>()?;
    let responses = tokio::task::block_in_place(move ||
        dedupe_files(first_file, 0..std::fs::metadata(first)?.len(), dest_reqs)
    )?;

    let mut files_errored = HashMap::<String, std::io::Error>::new();
    let mut files_affected = Vec::<String>::new();
    let mut total_bytes_saved = 0;

    for (file, response) in responses {
        match response {
            DedupeResponse::RangeSame { bytes_deduped } => {
                files_affected.push(file);
                total_bytes_saved += bytes_deduped;
            }
            DedupeResponse::Error(e) => {
                files_errored.insert(file, e);
            }
            DedupeResponse::RangeDiffers => {
                // does nothing, we don't care if this occurred
            }
        }
    }

    Ok(DedupeResult {
        file_targeted: first.clone(),
        files_errored,
        files_affected,
        total_bytes_saved,
    })
}

fn print_task_completion(result: Result<DedupeResult, DedupeError>) {
    match result {
        Ok(dedupe) => {
            eprintln!("==> De-dupe Targeting {}", dedupe.file_targeted);
            if dedupe.files_affected.len() > 0 {
                eprintln!(
                    "Saved {}B by re-using content in:",
                    SizeFormatterBinary::new(dedupe.total_bytes_saved),
                );
                for affected in dedupe.files_affected {
                    eprintln!("    {}", affected);
                }
            }
            if dedupe.files_errored.len() > 0 {
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
struct DedupeResult {
    file_targeted: String,
    files_errored: HashMap<String, std::io::Error>,
    files_affected: Vec<String>,
    total_bytes_saved: u64,
}

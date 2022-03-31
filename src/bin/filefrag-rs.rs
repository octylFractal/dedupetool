#![deny(warnings)]
//! A bare re-implementation of the `filefrag` command using Rust. Does not support any flags,
//! just reads the extents in the given files and outputs all info.

use std::path::{Path, PathBuf};

use clap::Parser;
use num_format::{Locale, ToFormattedString};

use dedupetool::ioctl_fiemap::get_extents;

/// Bare-bones filefrag command.
#[derive(Parser)]
#[clap(name = "filefrag-rs", version)]
struct FileFrag {
    /// The files to print information for.
    #[clap(parse(from_os_str), min_values = 1)]
    files: Vec<PathBuf>,
}

fn main() {
    let args: FileFrag = FileFrag::parse();

    for path in args.files {
        if let Err(e) = print_file(&path) {
            eprintln!("Failed to print information for {}: {}", path.display(), e);
        }
    }
}

fn print_file(path: &Path) -> Result<(), std::io::Error> {
    println!(
        "File size of {} is {}",
        path.display(),
        path.metadata()?.len()
    );

    let file = std::fs::File::open(path)?;
    let extents = get_extents(&file, 0..u64::MAX, false)?;
    for (i, extent) in extents.into_iter().enumerate() {
        println!("Extent #{}", i);
        println!(
            "    Logical range: {} - {}",
            extent.logical_offset.to_formatted_string(&Locale::en),
            (extent.logical_offset + extent.length).to_formatted_string(&Locale::en)
        );
        println!(
            "    Physical range: {} - {}",
            extent.physical_offset.to_formatted_string(&Locale::en),
            (extent.physical_offset + extent.length).to_formatted_string(&Locale::en)
        );
        println!("    Length: {}", extent.length.to_formatted_string(&Locale::en));
        println!(
            "    Flags: {}",
            extent
                .flags
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    Ok(())
}

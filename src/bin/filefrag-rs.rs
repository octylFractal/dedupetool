#![deny(warnings)]
//! A bare re-implementation of the `filefrag` command using Rust. Does not support any flags,
//! just reads the extents in the given files and outputs all info.

use std::path::{Path, PathBuf};

use num_format::{SystemLocale, ToFormattedString};
use structopt::StructOpt;

use dedupetool::ioctl_fiemap::get_extents;

#[derive(StructOpt)]
#[structopt(name = "filefrag-rs", about = "Bare-bones filefrag command")]
struct FileFrag {
    #[structopt(
        parse(from_os_str),
        help = "The files to print information for",
        min_values = 1
    )]
    files: Vec<PathBuf>,
}

fn main() {
    let args: FileFrag = FileFrag::from_args();

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
    let locale = SystemLocale::default().unwrap();
    for (i, extent) in extents.into_iter().enumerate() {
        println!("Extent #{}", i);
        println!(
            "    Logical range: {} - {}",
            extent.logical_offset.to_formatted_string(&locale),
            (extent.logical_offset + extent.length).to_formatted_string(&locale)
        );
        println!(
            "    Physical range: {} - {}",
            extent.physical_offset.to_formatted_string(&locale),
            (extent.physical_offset + extent.length).to_formatted_string(&locale)
        );
        println!("    Length: {}", extent.length.to_formatted_string(&locale));
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

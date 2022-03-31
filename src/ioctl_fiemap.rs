use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::ops::Range;

use crate::ioctl::ioctl;
use crate::ioctl_consts::*;

/// Get extents for a [file], approximately within the given [range].
/// If [sync] is `true`, will set `FIEMAP_FLAG_SYNC`.
/// Returns all extents that touch the given range, not just the ones strictly within it.
pub fn get_extents(
    file: &std::fs::File,
    range: Range<u64>,
    sync: bool,
) -> Result<Vec<Extent>, std::io::Error> {
    let flags = if sync { FIEMAP_FLAG_SYNC } else { 0 };
    let mut extents = Vec::<Extent>::new();
    let mut offset: u64 = range.start;
    while offset < range.end {
        let mut request = FileExtentMapRequest::new(offset..range.end, flags);

        ioctl(file, FS_IOC_FIEMAP, &mut request)?;

        let valid_extents: &[FileExtent] =
            &request.fm_extents[0..(request.fm_mapped_extents as usize)];
        for extent in valid_extents {
            extents.push(Extent {
                logical_offset: extent.fe_logical,
                physical_offset: extent.fe_physical,
                length: extent.fe_length,
                flags: ExtentFlag::set_from(extent.fe_flags),
            });
        }

        if offset == 0 && valid_extents.is_empty() {
            // empty file
            return Ok(Vec::new());
        }

        let last = valid_extents.last().ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidData,
                "File had no extents in range, probably changed while reading!",
            )
        })?;
        if (last.fe_flags & FIEMAP_EXTENT_LAST) != 0 {
            break;
        }
        // Move offset to the end of the extent we just saw
        offset = last.fe_logical + last.fe_length;
    }
    Ok(extents)
}

pub struct Extent {
    pub logical_offset: u64,
    pub physical_offset: u64,
    pub length: u64,
    pub flags: BTreeSet<ExtentFlag>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ExtentFlag {
    Last,
    LocationUnknown,
    DelayedAllocation,
    Encoded,
    DataEncrypted,
    NotAligned,
    DataInline,
    DataTail,
    Unwritten,
    Merged,
    Shared,
    /// This one is different, it marks an unknown extent flag and holds the value.
    Unknown(u32),
}

impl Display for ExtentFlag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtentFlag::Unknown(flag) => f.write_fmt(format_args!("{:#x}", flag)),
            _ => std::fmt::Debug::fmt(&self, f),
        }
    }
}

macro_rules! include_flag {
    ($set:expr, $flags:expr, $flag:expr, $enum_val:expr) => {
        if ($flags & $flag) != 0 {
            $set.insert($enum_val);
            $flags &= !$flag;
        }
    };
}

impl ExtentFlag {
    fn set_from(mut flags: u32) -> BTreeSet<ExtentFlag> {
        let mut set = BTreeSet::<ExtentFlag>::new();
        include_flag!(set, flags, FIEMAP_EXTENT_LAST, ExtentFlag::Last);
        include_flag!(
            set,
            flags,
            FIEMAP_EXTENT_UNKNOWN,
            ExtentFlag::LocationUnknown
        );
        include_flag!(
            set,
            flags,
            FIEMAP_EXTENT_DELALLOC,
            ExtentFlag::DelayedAllocation
        );
        include_flag!(set, flags, FIEMAP_EXTENT_ENCODED, ExtentFlag::Encoded);
        include_flag!(
            set,
            flags,
            FIEMAP_EXTENT_DATA_ENCRYPTED,
            ExtentFlag::DataEncrypted
        );
        include_flag!(
            set,
            flags,
            FIEMAP_EXTENT_NOT_ALIGNED,
            ExtentFlag::NotAligned
        );
        include_flag!(
            set,
            flags,
            FIEMAP_EXTENT_DATA_INLINE,
            ExtentFlag::DataInline
        );
        include_flag!(set, flags, FIEMAP_EXTENT_DATA_TAIL, ExtentFlag::DataTail);
        include_flag!(set, flags, FIEMAP_EXTENT_UNWRITTEN, ExtentFlag::Unwritten);
        include_flag!(set, flags, FIEMAP_EXTENT_MERGED, ExtentFlag::Merged);
        include_flag!(set, flags, FIEMAP_EXTENT_SHARED, ExtentFlag::Shared);

        for x in 0..32 {
            let flag = 1 << x;
            include_flag!(set, flags, flag, ExtentFlag::Unknown(flag));
        }

        set
    }
}

const FILE_EXTENT_COUNT: usize = 512;

#[repr(C)]
struct FileExtentMapRequest {
    fm_start: u64,
    fm_length: u64,
    fm_flags: u32,
    fm_mapped_extents: u32,
    fm_extent_count: u32,
    fm_reserved: u32,
    fm_extents: [FileExtent; FILE_EXTENT_COUNT],
}

impl FileExtentMapRequest {
    fn new(range: Range<u64>, flags: u32) -> FileExtentMapRequest {
        FileExtentMapRequest {
            fm_start: range.start,
            fm_length: range.end - range.start,
            fm_flags: flags,
            fm_mapped_extents: 0,
            fm_extent_count: FILE_EXTENT_COUNT as u32,
            fm_reserved: 0,
            fm_extents: [Default::default(); FILE_EXTENT_COUNT],
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct FileExtent {
    fe_logical: u64,
    fe_physical: u64,
    fe_length: u64,
    fe_reserved64: [u64; 2],
    fe_flags: u32,
    fe_reserved: [u32; 3],
}

impl Default for FileExtent {
    fn default() -> Self {
        FileExtent {
            fe_logical: u64::MAX,
            fe_physical: u64::MAX,
            fe_length: u64::MAX,
            fe_reserved64: [0; 2],
            fe_flags: u32::MAX,
            fe_reserved: [0; 3],
        }
    }
}

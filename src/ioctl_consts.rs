// Generated from export_ioctl_constants.cpp -- DO NOT EDIT DIRECTLY!
use std::os::raw::c_ulong;

pub const FIDEDUPERANGE: c_ulong = 0xc0189436;
pub const FILE_DEDUPE_RANGE_DIFFERS: i32 = 0x1;
pub const FILE_DEDUPE_RANGE_SAME: i32 = 0x0;
pub const FS_IOC_FIEMAP: c_ulong = 0xc020660b;
pub const FIEMAP_FLAG_SYNC: u32 = 0x1;
pub const FIEMAP_EXTENT_LAST: u32 = 0x1;
pub const FIEMAP_EXTENT_UNKNOWN: u32 = 0x2;
pub const FIEMAP_EXTENT_DELALLOC: u32 = 0x4;
pub const FIEMAP_EXTENT_ENCODED: u32 = 0x8;
pub const FIEMAP_EXTENT_DATA_ENCRYPTED: u32 = 0x80;
pub const FIEMAP_EXTENT_NOT_ALIGNED: u32 = 0x100;
pub const FIEMAP_EXTENT_DATA_INLINE: u32 = 0x200;
pub const FIEMAP_EXTENT_DATA_TAIL: u32 = 0x400;
pub const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x800;
pub const FIEMAP_EXTENT_MERGED: u32 = 0x1000;
pub const FIEMAP_EXTENT_SHARED: u32 = 0x2000;

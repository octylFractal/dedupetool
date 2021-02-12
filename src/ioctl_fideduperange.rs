//! An tiny wrapper over the FIDEDUPERANGE ioctl.

use std::collections::HashMap;
use std::hash::Hash;
use std::mem::size_of;
use std::ops::Range;
use std::os::unix::io::{AsRawFd, RawFd};

use libc::ioctl;

use dedupetool_sys::*;

/// Dedupes [src]'s bytes from other files ([request]).
///
/// Destination files go in [request], keyed by whatever you wish. Results will be reported
/// under the same keys.
pub fn dedupe_files<K: Eq + Hash + Clone>(
    src: std::fs::File,
    src_range: Range<u64>,
    request: HashMap<K, DedupeRequest>,
) -> Result<HashMap<K, DedupeResponse>, std::io::Error> {
    // flush files to sync extent mapping
    src.sync_all()?;
    for x in request.values() {
        x.dest.sync_all()?;
    }

    let fd_map: HashMap<RawFd, K> = request
        .keys()
        .map(|k| (request[k].dest.as_raw_fd(), k.clone()))
        .collect();
    let mut dedupe_request_info: Vec<DedupeRequestInternalInfo> = request
        .values()
        .map(|r| DedupeRequestInternalInfo {
            dest_fd: r.dest.as_raw_fd() as i64,
            dest_offset: r.dest_offset,
            // Purposefully throw junk in the return values
            // That way, if for some reason they don't get filled, we know
            bytes_deduped: u64::MIN,
            status: i32::MAX,
            reserved: 0,
        })
        .collect();
    let request_internal = DedupeRequestInternal {
        src_offset: src_range.start,
        src_length: src_range.end - src_range.start,
        dest_count: dedupe_request_info.len() as u16,
        reserved1: 0,
        reserved2: 0,
    };
    let result = unsafe {
        // I'm going MAD with POWER!
        let memsize = size_of::<DedupeRequestInternal>()
            + size_of::<DedupeRequestInternalInfo>() * dedupe_request_info.len();
        let memchunk = libc::malloc(memsize);
        if memchunk.is_null() {
            panic!("Couldn't malloc data for the request!");
        }
        let req_ptr = memchunk.cast::<DedupeRequestInternal>();
        // push in the request at the front
        req_ptr.write(request_internal);

        // fill in the """array"""
        let array_base = req_ptr.add(1).cast::<DedupeRequestInternalInfo>();
        let mut array = array_base;
        for x in dedupe_request_info.iter() {
            array.write(x.clone());
            array = array.add(1);
        }

        let result = ioctl(src.as_raw_fd(), *FIDEDUPERANGE, memchunk);

        // copy back results
        array = array_base;
        for x in dedupe_request_info.iter_mut() {
            *x = array.read();
            array = array.add(1);
        }

        libc::free(memchunk);
        result
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(dedupe_request_info
        .into_iter()
        .map(|info| {
            let response = match info.status {
                errno if errno < 0 => {
                    DedupeResponse::Error(std::io::Error::from_raw_os_error(-errno))
                }
                x if x == *FILE_DEDUPE_RANGE_DIFFERS as i32 => DedupeResponse::RangeDiffers,
                x if x == *FILE_DEDUPE_RANGE_SAME as i32 => {
                    if info.bytes_deduped == 0 {
                        // I guess this is also RangeDiffers?
                        DedupeResponse::RangeDiffers
                    } else {
                        DedupeResponse::RangeSame {
                            bytes_deduped: info.bytes_deduped,
                        }
                    }
                }
                unknown => panic!("Unknown status from FIDEDUPERANGE ioctl: {}", unknown),
            };
            (fd_map[&(info.dest_fd as RawFd)].clone(), response)
        })
        .collect())
}

pub struct DedupeRequest {
    dest: std::fs::File,
    dest_offset: u64,
}

impl DedupeRequest {
    pub fn new(dest: std::fs::File, offset: u64) -> DedupeRequest {
        DedupeRequest {
            dest,
            dest_offset: offset,
        }
    }
}

pub enum DedupeResponse {
    Error(std::io::Error),
    RangeDiffers,
    RangeSame { bytes_deduped: u64 },
}

#[repr(C)]
struct DedupeRequestInternal {
    src_offset: u64,
    src_length: u64,
    dest_count: u16,
    reserved1: u16,
    reserved2: u32,
}

#[repr(C)]
#[derive(Clone)]
struct DedupeRequestInternalInfo {
    dest_fd: i64,
    dest_offset: u64,
    bytes_deduped: u64,
    status: i32,
    reserved: u32,
}

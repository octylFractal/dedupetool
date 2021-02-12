//! An tiny wrapper over the FIDEDUPERANGE ioctl.

use std::collections::HashMap;
use std::hash::Hash;
use std::mem::size_of;
use std::ops::Range;
use std::os::unix::io::{AsRawFd, RawFd};

use libc::ioctl;

use dedupetool_sys::*;

/// This is just a number I came up with. The max combined size needs to be less than a page,
/// so (4096 <page> - 24 <sizeof request internal>) / 32 <sizeof request internal info> = 127
/// Rounding down in case other systems have padding -> 100.
const IOCTL_DEDUPE_MAX_DESTS: usize = 100;

/// Doing a dedupe with this little data is probably too expensive.
const IOCTL_DEDUPE_MIN_BYTES: u64 = 16 * 1024;

/// We're only likely to be able to dedupe this much at once. See ioctl_fideduperange(2) for why.
const IOCTL_DEDUPE_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Dedupes [src]'s bytes from other files ([request]).
///
/// Destination files go in [request], keyed by whatever you wish. Results will be reported
/// under the same keys.
pub fn dedupe_files<K: Eq + Hash + Clone>(
    src: std::fs::File,
    src_range: Range<u64>,
    request: HashMap<K, DedupeRequest>,
) -> Result<HashMap<K, Vec<DedupeResponse>>, std::io::Error> {
    if (src_range.end - src_range.start) < IOCTL_DEDUPE_MIN_BYTES {
        return Ok(request
            .into_iter()
            .map(|(k, _)| (k, vec![DedupeResponse::RangeTooSmall]))
            .collect());
    }
    let fd_map: HashMap<RawFd, K> = request
        .keys()
        .map(|k| (request[k].dest.as_raw_fd(), k.clone()))
        .collect();

    let full_length = src_range.end - src_range.start;
    let mut offset = 0;
    let mut aggregate_results = HashMap::<K, Vec<DedupeResponse>>::new();
    while offset < full_length {
        for req_chunk in request
            .values()
            .collect::<Vec<_>>()
            .chunks(IOCTL_DEDUPE_MAX_DESTS)
        {
            let request_internal = DedupeRequestInternal {
                src_offset: src_range.start + offset,
                src_length: u64::min(
                    src_range.end - (src_range.start + offset),
                    IOCTL_DEDUPE_MAX_BYTES,
                ),
                dest_count: req_chunk.len() as u16,
                reserved1: 0,
                reserved2: 0,
            };
            let mut infos = req_chunk
                .iter()
                .map(|r| DedupeRequestInternalInfo {
                    dest_fd: r.dest.as_raw_fd() as i64,
                    dest_offset: r.dest_offset + offset,
                    // Purposefully throw junk in the return values
                    // That way, if for some reason they don't get filled, we know
                    bytes_deduped: u64::MIN,
                    status: i32::MAX,
                    reserved: 0,
                })
                .collect::<Vec<_>>();
            call_ioctl_unsafe(&src, request_internal, &mut infos)?;

            for info in infos {
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
                let vec = aggregate_results
                    .entry(fd_map[&(info.dest_fd as RawFd)].clone())
                    .or_insert_with(|| vec![]);
                vec.push(response);
            }
        }

        offset += IOCTL_DEDUPE_MAX_BYTES;
    }

    // flush files to sync extent mapping
    src.sync_all()?;
    for x in request.values() {
        x.dest.sync_all()?;
    }

    Ok(aggregate_results)
}

fn call_ioctl_unsafe(
    src: &std::fs::File,
    request_internal: DedupeRequestInternal,
    infos: &mut [DedupeRequestInternalInfo],
) -> Result<(), std::io::Error> {
    let result = unsafe {
        // I'm going MAD with POWER!
        let memsize = size_of::<DedupeRequestInternal>()
            + size_of::<DedupeRequestInternalInfo>() * (request_internal.dest_count) as usize;
        let memchunk = libc::malloc(memsize);
        if memchunk.is_null() {
            panic!("Couldn't malloc data for the request!");
        }
        let req_ptr = memchunk.cast::<DedupeRequestInternal>();
        // push in the request at the front
        req_ptr.write(request_internal.clone());

        // fill in the """array"""
        let array_base = req_ptr.add(1).cast::<DedupeRequestInternalInfo>();
        let mut array = array_base;
        for x in infos.iter() {
            array.write(x.clone());
            array = array.add(1);
        }

        let result = ioctl(src.as_raw_fd(), *FIDEDUPERANGE, memchunk);

        // copy back results
        array = array_base;
        for x in infos.iter_mut() {
            *x = array.read();
            array = array.add(1);
        }

        libc::free(memchunk);
        result
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
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
    RangeTooSmall,
    RangeDiffers,
    RangeSame { bytes_deduped: u64 },
}

#[derive(Clone)]
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

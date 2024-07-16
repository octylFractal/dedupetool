//! An tiny wrapper over the FIDEDUPERANGE ioctl.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::mem::size_of;
use std::ops::Range;
use std::os::linux::fs::MetadataExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

use crate::ioctl::ioctl;
use crate::ioctl_consts::*;

/// This is just a number I came up with. The max combined size needs to be less than a page,
/// so (4096 <page> - 24 <sizeof request internal>) / 32 <sizeof request internal info> = 127
/// Rounding down in case other systems have padding -> 100.
const IOCTL_DEDUPE_MAX_DESTS: usize = 100;

/// We're only likely to be able to dedupe this much at once. See ioctl_fideduperange(2) for why.
const IOCTL_DEDUPE_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Dedupes [src]'s bytes from other files ([request]).
///
/// Destination files go in [request], keyed by whatever you wish. Results will be reported
/// under the same keys.
#[allow(warnings)]
pub fn dedupe_files<K: Eq + Hash + Clone>(
    src: &std::fs::File,
    src_range: Range<u64>,
    request: HashMap<K, DedupeRequest>,
) -> Result<HashMap<K, Vec<DedupeResponse>>, std::io::Error> {
    let metadata = src.metadata()?;
    let block_size = metadata.st_blksize();
    fn align_down(n: u64, align: u64) -> u64 {
        n - ((n * align) / align)
    }
    fn align_up(n: u64, align: u64) -> u64 {
        ((n + align - 1) / align) * align
    }

    let full_length = src_range.end - src_range.start;
    let mut offset = 0;
    let mut aggregate_results = HashMap::<K, Vec<DedupeResponse>>::new();
    while offset < full_length {
        for req_chunk in request
            .iter()
            .collect::<Vec<_>>()
            .chunks(IOCTL_DEDUPE_MAX_DESTS)
        {
            let open_fds = req_chunk
                .iter()
                .map(|(_, r)| {
                    OpenOptions::new()
                        .write(true)
                        .open(&r.dest)
                        .map(|f| (r.dest.clone(), f))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            let fd_map: HashMap<RawFd, K> = req_chunk
                .iter()
                .map(|(k, r)| (open_fds[&r.dest].as_raw_fd(), K::clone(k)))
                .collect();
            let mut request_internal = DedupeRequestInternal {
                src_offset: align_down(src_range.start + offset, block_size),
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
                .map(|(_, r)| DedupeRequestInternalInfo {
                    dest_fd: open_fds[&r.dest].as_raw_fd() as i64,
                    dest_offset: align_down(r.dest_offset + offset, block_size),
                    // Purposefully throw junk in the return values
                    // That way, if for some reason they don't get filled, we know
                    bytes_deduped: u64::MIN,
                    status: i32::MAX,
                    reserved: 0,
                })
                .collect::<Vec<_>>();
            call_ioctl_unsafe(src, request_internal, &mut infos)?;

            for info in infos {
                let response = match info.status {
                    errno if errno < 0 => {
                        DedupeResponse::Error(std::io::Error::from_raw_os_error(-errno))
                    }
                    FILE_DEDUPE_RANGE_DIFFERS => DedupeResponse::RangeDiffers,
                    FILE_DEDUPE_RANGE_SAME => {
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
                    .or_insert_with(Vec::new);
                vec.push(response);
            }
        }

        offset += IOCTL_DEDUPE_MAX_BYTES;
    }

    Ok(aggregate_results)
}

fn call_ioctl_unsafe(
    src: &std::fs::File,
    request_internal: DedupeRequestInternal,
    infos: &mut [DedupeRequestInternalInfo],
) -> Result<(), std::io::Error> {
    unsafe {
        // I'm going MAD with POWER!
        let memsize = size_of::<DedupeRequestInternal>()
            + size_of::<DedupeRequestInternalInfo>() * (request_internal.dest_count) as usize;
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
        for x in infos.iter() {
            array.write(x.clone());
            array = array.add(1);
        }

        // `unwrap` is fine as we just checked for null
        let result = ioctl(src, FIDEDUPERANGE, memchunk.as_mut().unwrap());

        if result.is_ok() {
            // copy back results
            array = array_base;
            for x in infos.iter_mut() {
                *x = array.read();
                array = array.add(1);
            }
        }

        libc::free(memchunk);
        result
    }
}

pub struct DedupeRequest {
    dest: PathBuf,
    dest_offset: u64,
}

impl DedupeRequest {
    pub fn new<P: AsRef<Path>>(dest: P, offset: u64) -> DedupeRequest {
        DedupeRequest {
            dest: dest.as_ref().to_path_buf(),
            dest_offset: offset,
        }
    }
}

pub enum DedupeResponse {
    Error(std::io::Error),
    RangeDiffers,
    RangeSame { bytes_deduped: u64 },
}

#[derive(Debug, Clone)]
#[repr(C)]
struct DedupeRequestInternal {
    src_offset: u64,
    src_length: u64,
    dest_count: u16,
    reserved1: u16,
    reserved2: u32,
}

#[repr(C)]
#[derive(Debug, Clone)]
struct DedupeRequestInternalInfo {
    dest_fd: i64,
    dest_offset: u64,
    bytes_deduped: u64,
    status: i32,
    reserved: u32,
}

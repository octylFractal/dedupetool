use std::os::raw::c_ulong;
use std::os::unix::io::AsRawFd;

pub fn ioctl<T>(src: &std::fs::File, request: c_ulong, data: *mut T) -> Result<(), std::io::Error> {
    if unsafe { libc::ioctl(src.as_raw_fd(), request, data) } == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

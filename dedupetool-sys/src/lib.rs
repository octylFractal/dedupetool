use std::os::raw::c_ulong;

use once_cell::sync::Lazy;

extern "C" {
    fn get_fideduperange() -> c_ulong;
    fn get_file_dedupe_range_differs() -> c_ulong;
    fn get_file_dedupe_range_same() -> c_ulong;
}

/// The FIDEDUPERANGE constant defined in `linux/fs.h`.
pub static FIDEDUPERANGE: Lazy<c_ulong> = Lazy::new(|| unsafe { get_fideduperange() });
/// The FILE_DEDUPE_RANGE_DIFFERS constant defined in `linux/fs.h`.
pub static FILE_DEDUPE_RANGE_DIFFERS: Lazy<c_ulong> =
    Lazy::new(|| unsafe { get_file_dedupe_range_differs() });
/// The FIDEDUPERANGE constant defined in `linux/fs.h`.
pub static FILE_DEDUPE_RANGE_SAME: Lazy<c_ulong> =
    Lazy::new(|| unsafe { get_file_dedupe_range_same() });

#![deny(warnings)]

pub mod diskblade;
pub mod ioctl;
pub mod ioctl_consts;
pub mod ioctl_fideduperange;
pub mod ioctl_fiemap;
pub mod termhelp;
mod tokio_futures_io;

// Technically this only requires >=32 bits, but that's not expressible in cfg()
#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("diskblade only supports 32-bit and 64-bit targets");

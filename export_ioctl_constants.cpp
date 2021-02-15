#include <iostream>
#include <fstream>
#include <linux/fs.h>
#include <linux/fiemap.h>

#define WRITE_CONST(TARGET, NAME, TYPE) TARGET << "pub const " #NAME ": " TYPE " = 0x" << std::hex << NAME << ";\n"

int main() {
    std::ofstream rust_file;
    rust_file.open("./src/ioctl_consts.rs");

    rust_file << "// Generated from export_ioctl_constants.cpp -- DO NOT EDIT DIRECTLY!\n";
    rust_file << "use std::os::raw::c_ulong;\n";
    rust_file << "\n";
    WRITE_CONST(rust_file, FIDEDUPERANGE, "c_ulong");
    WRITE_CONST(rust_file, FILE_DEDUPE_RANGE_DIFFERS, "i32");
    WRITE_CONST(rust_file, FILE_DEDUPE_RANGE_SAME, "i32");

    WRITE_CONST(rust_file, FS_IOC_FIEMAP, "c_ulong");
    WRITE_CONST(rust_file, FIEMAP_FLAG_SYNC, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_LAST, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_UNKNOWN, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_DELALLOC, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_ENCODED, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_DATA_ENCRYPTED, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_NOT_ALIGNED, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_DATA_INLINE, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_DATA_TAIL, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_UNWRITTEN, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_MERGED, "u32");
    WRITE_CONST(rust_file, FIEMAP_EXTENT_SHARED, "u32");

    rust_file.close();
    return 0;
}

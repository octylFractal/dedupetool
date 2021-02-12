#include <linux/fs.h>

unsigned long get_fideduperange() {
    return FIDEDUPERANGE;
}

unsigned long get_file_dedupe_range_differs() {
    return FILE_DEDUPE_RANGE_DIFFERS;
}

unsigned long get_file_dedupe_range_same() {
    return FILE_DEDUPE_RANGE_SAME;
}

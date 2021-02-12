fn main() {
    let inputs = vec!["csrc/ioctl_consts.c"];
    cc::Build::new()
        .files(&inputs)
        .warnings_into_errors(true)
        .compile("dedupetool");

    for input in inputs {
        println!("cargo:rerun-if-changed={}", input);
    }
}

use std::path::PathBuf;

/// A target to deduplicate.
#[derive(Debug, Clone)]
pub struct FileSectionTarget {
    /// The length to deduplicate after each offset in [`offsets`].
    pub length: u64,
    /// The file offsets to de-dupe at.
    pub offsets: Vec<FileOffset>,
}

/// An offset into a file.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FileOffset {
    /// The file this is an offset into.
    file: PathBuf,
    /// The offset into the file of the section.
    offset: u64,
}

impl FileOffset {
    pub fn new(file: PathBuf, offset: u64) -> Self {
        // normalize file
        let file = file.canonicalize().expect("file should exist");
        Self { file, offset }
    }

    pub fn file(&self) -> &PathBuf {
        &self.file
    }

    pub fn into_file(self) -> PathBuf {
        self.file
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

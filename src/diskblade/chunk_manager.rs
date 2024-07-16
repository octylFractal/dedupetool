use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::PathBuf;

use parse_display::Display;
use rangemap::RangeMap;

use crate::diskblade::{FileOffset, FileSectionTarget};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Chunk {
    pub hash: u64,
    pub offset: u64,
    pub length: u32,
}

#[derive(Display, Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
struct ChunkIndex(usize);

#[derive(Display, Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
struct PathIndex(usize);

#[derive(Display, Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
struct ChunkHash(u64);

#[derive(Display, Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
struct ChunkLength(u32);

#[derive(Default, Debug)]
pub struct ChunkManager {
    paths: Vec<PathBuf>,
    chunk_data: Vec<Chunk>,
    path_to_chunk_indices: HashMap<PathIndex, Range<ChunkIndex>>,
    chunk_index_to_path: RangeMap<ChunkIndex, PathIndex>,
    /// (hash, len) -> index in [`chunk_data`] of matching chunk
    hash_to_chunk_index: HashMap<(ChunkHash, ChunkLength), HashSet<ChunkIndex>>,
}

impl ChunkManager {
    pub fn push_path(&mut self, path: PathBuf, chunks: Vec<Chunk>) {
        let path_start = self.chunk_data.len();
        for chunk in chunks {
            let hash = ChunkHash(chunk.hash);
            let length = ChunkLength(chunk.length);
            self.chunk_data.push(chunk);
            let index = ChunkIndex(self.chunk_data.len() - 1);
            self.hash_to_chunk_index
                .entry((hash, length))
                .or_default()
                .insert(index);
        }
        let path_end = self.chunk_data.len();
        let range = ChunkIndex(path_start)..ChunkIndex(path_end);
        let path_index = PathIndex(self.paths.len());
        self.paths.push(path);
        self.path_to_chunk_indices.insert(path_index, range.clone());
        self.chunk_index_to_path.insert(range, path_index);
    }

    pub fn into_file_section_targets(mut self) -> Vec<FileSectionTarget> {
        self.hash_to_chunk_index.retain(|_, v| {
            // remove all but one chunk that is part of the same file
            let mut files = HashSet::new();
            v.retain(|index| {
                let file = self.chunk_index_to_path.get(index).unwrap();
                files.insert(file)
            });
            // drop empty / size one hash groups, we don't care about them for deduplication
            v.len() > 1
        });
        self.hash_to_chunk_index.shrink_to_fit();

        // Goal: merge as many chunks as possible into a single large chunk
        // 1. Go through each path, and its chunks
        // 2. See how many chunks we can take, preferring to be longer rather than deduplicate more files
        // 3. Once there are no more shared chunks, split that as a new group and move on

        let strings = make_hash_tea((0..self.paths.len()).map(|i| {
            let Range { start, end } = self.path_to_chunk_indices[&PathIndex(i)];
            self.chunk_data[start.0..end.0].iter().copied()
        }));
        // merge_common_strings(&mut strings);

        eprintln!("strings: {:?}", strings);

        let mut new_groups = Vec::<FileSectionTarget>::new();

        let iter = 0..self.paths.len();
        #[cfg(not(test))]
        let iter = {
            use crate::termhelp::DedupetoolProgressBar;
            use indicatif::{ProgressBar, ProgressFinish, ProgressIterator};
            iter.progress_with(
                ProgressBar::new(self.paths.len() as u64)
                    .with_steady_tick_dedupetool()
                    .with_style_dedupetool()
                    .with_message("Merging chunk(s)...")
                    .with_finish(ProgressFinish::WithMessage("Merged chunk(s)".into())),
            )
        };
        for index in iter {
            let index = PathIndex(index);
            let range = self.path_to_chunk_indices[&index].clone();
            // What paths are we using in this group? Which chunk do they start at?
            let mut start_chunks = HashMap::<PathIndex, ChunkIndex>::new();
            let mut group_and_reset = |this: &mut Self,
                                       start_chunks: &mut HashMap<PathIndex, ChunkIndex>,
                                       chunk_index: usize| {
                if start_chunks.len() >= 2 {
                    let target = this.create_target(index, start_chunks, chunk_index);
                    new_groups.push(target);
                    *start_chunks = HashMap::new();
                }
            };
            for chunk_index in range.start.0..range.end.0 {
                let chunk = &self.chunk_data[chunk_index];
                let other_chunks = match self
                    .hash_to_chunk_index
                    .get(&(ChunkHash(chunk.hash), ChunkLength(chunk.length)))
                {
                    Some(chunks) if chunks.contains(&ChunkIndex(chunk_index)) => chunks,
                    _ => {
                        eprintln!(
                            "{:?} grouping due to missing hash: {:?}",
                            self.paths[index.0], start_chunks
                        );
                        group_and_reset(&mut self, &mut start_chunks, chunk_index);
                        continue;
                    }
                };
                // drop paths we don't see
                let current_chunks = other_chunks
                    .iter()
                    .map(|index| {
                        (
                            self.chunk_index_to_path.get(index).copied().unwrap(),
                            *index,
                        )
                    })
                    .collect::<HashMap<_, _>>();
                if start_chunks.is_empty() {
                    // We haven't started work on a set yet, so start one
                    start_chunks = current_chunks;
                    continue;
                }
                // collect from the start chunks using the keys of the current set
                let new_chunks = current_chunks
                    .keys()
                    .filter_map(|path| start_chunks.get(path).copied().map(|index| (*path, index)))
                    .collect::<HashMap<_, _>>();
                if new_chunks.len() < 2 {
                    eprintln!(
                        "{:?} grouping due to full narrowing: {:?}",
                        self.paths[index.0], start_chunks
                    );
                    // we reached the end of the group, start again
                    group_and_reset(&mut self, &mut start_chunks, chunk_index);
                    // start a new group with the current chunks
                    start_chunks = current_chunks;
                    continue;
                }
                // we have our new set to track
                start_chunks = new_chunks;
            }

            // cleanup the last group
            eprintln!(
                "{:?} grouping last: {:?}",
                self.paths[index.0], start_chunks
            );
            group_and_reset(&mut self, &mut start_chunks, range.end.0);
        }

        new_groups
    }

    fn create_target(
        &mut self,
        index: PathIndex,
        start_chunks: &mut HashMap<PathIndex, ChunkIndex>,
        chunk_index: usize,
    ) -> FileSectionTarget {
        let first_index = start_chunks.get(&index).unwrap().0;
        let last_index = chunk_index - 1;

        // Remove the chunks we're including here from the hash map, except for those belonging to
        // the current file (so we can dedupe from it to the other files not included here)
        let offset = last_index + 1 - first_index;
        for &path in start_chunks.keys() {
            if path == index {
                continue;
            }
            let start_chunk = start_chunks[&path];
            for c in start_chunk.0..(start_chunk.0 + offset) {
                let chunk = &self.chunk_data[c];
                let hash = ChunkHash(chunk.hash);
                let length = ChunkLength(chunk.length);
                let Entry::Occupied(mut v) = self.hash_to_chunk_index.entry((hash, length)) else {
                    continue;
                };
                let set = v.get_mut();
                set.remove(&ChunkIndex(c));
                if set.is_empty() {
                    v.remove_entry();
                }
            }
        }

        let first_chunk = &self.chunk_data[first_index];
        let last_chunk = &self.chunk_data[last_index];
        let length = last_chunk.offset + last_chunk.length as u64 - first_chunk.offset;
        let offsets = start_chunks
            .iter_mut()
            .map(|(path, chunk)| FileOffset {
                file: self.paths[path.0].clone(),
                offset: self.chunk_data[chunk.0].offset,
            })
            .collect();
        FileSectionTarget { length, offsets }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Eq, PartialEq)]
enum HashElem {
    Original(ChunkHash, ChunkLength),
    Merged(Box<[ChunkHash]>, ChunkLength),
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct HashString {
    elems: Vec<HashElem>,
}

// impl TeaString for HashString {
//     type Item = HashElem;
//
//     fn len(&self) -> usize {
//         self.elems.len()
//     }
//
//     fn get(&self, index: usize) -> Option<&Self::Item> {
//         self.elems.get(index)
//     }
//
//     fn merge_range(&mut self, range: Range<usize>) {
//         if range.len() < 2 {
//             return;
//         }
//         let taken = self.elems.drain(range.clone()).collect::<Vec<_>>();
//         let mut hashes = Vec::with_capacity(taken.iter().fold(0, |acc, elem| {
//             acc + match elem {
//                 HashElem::Original(_, _) => 1,
//                 HashElem::Merged(hashes, _) => hashes.len(),
//             }
//         }));
//         let mut length = 0;
//         for elem in taken {
//             match elem {
//                 HashElem::Original(hash, len) => {
//                     hashes.push(hash);
//                     length += len.0;
//                 }
//                 HashElem::Merged(new_hashes, len) => {
//                     hashes.extend_from_slice(&new_hashes);
//                     length += len.0;
//                 }
//             }
//         }
//         self.elems.insert(
//             range.start,
//             HashElem::Merged(hashes.into_boxed_slice(), ChunkLength(length)),
//         );
//     }
// }

fn make_hash_tea(
    string_sources: impl Iterator<Item = impl IntoIterator<Item = Chunk>>,
) -> Vec<HashString> {
    let mut strings = Vec::new();
    for string in string_sources {
        let mut elems = Vec::new();
        for chunk in string {
            let hash = ChunkHash(chunk.hash);
            let length = ChunkLength(chunk.length);
            elems.push(HashElem::Original(hash, length));
        }
        strings.push(HashString { elems });
    }
    strings
}

impl FromIterator<(PathBuf, Vec<Chunk>)> for ChunkManager {
    fn from_iter<T: IntoIterator<Item = (PathBuf, Vec<Chunk>)>>(iter: T) -> Self {
        let mut manager = ChunkManager::default();
        for (path, chunks) in iter {
            manager.push_path(path, chunks);
        }
        manager
    }
}

#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::iter;
use std::num::NonZeroUsize;

pub trait TeaString {
    type Item<'a>
    where
        Self: 'a;

    fn len(&self) -> usize;

    fn get(&self, index: usize) -> Option<Self::Item<'_>>;
}

pub trait Mergeable<T>: PartialEq {
    type Output;

    fn merge(items: &[T]) -> Self::Output;
}

#[allow(unreachable_code)] // needed to make rust shut up about the iterator being invalid
/// Given a set of strings made of `T`s, take the longest shared sequence possible and merge it
/// into a single item. Repeat until all sequences of size 2 or greater are merged.
/// Returns an iterator of each item and the index of the string it came from.
///
/// A special case is where equivalent `T`s are found in the same string. In this case, they will
/// be merged twice, once for each `T`.
pub fn merge_common_strings<S: TeaString + for<'a> From<Vec<S::Item<'a>>> + Debug, O>(
    strings: &mut [S],
) -> impl Iterator<Item = (O, usize)>
where
    for<'a> S::Item<'a>: Mergeable<O>,
{
    todo!("strings: {:?}", strings);
    iter::empty()
}

struct UniqueTeaString<S> {
    inner: S,
    /// Map from index to count, if greater than 1. Done this way since most items won't be
    /// duplicate.
    items: HashMap<usize, NonZeroUsize>,
}

impl<S> TeaString for UniqueTeaString<S>
where
    S: TeaString,
{
    type Item<'a> = (usize, S::Item<'a>) where S: 'a;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn get(&self, index: usize) -> Option<Self::Item<'_>> {
        self.inner.get(index).map(|item| {
            let count = self
                .items
                .get(&index)
                .copied()
                .unwrap_or(NonZeroUsize::new(1).unwrap())
                .get();
            (count, item)
        })
    }
}

fn gen_unique_strings<S: TeaString>(_strings: &[S]) -> Vec<UniqueTeaString<S>> {
    todo!()
}

/*
#[cfg(test)]
mod test {
    use super::TeaString;
    use crate::diskblade::tea_merger::merge_common_strings;
    use std::ops::Range;

    #[derive(Debug, Clone, Eq, PartialEq)]
    enum TestElem {
        Original(u32),
        Merged(Box<[u32]>),
    }

    impl From<u32> for TestElem {
        fn from(value: u32) -> Self {
            TestElem::Original(value)
        }
    }

    impl From<Vec<u32>> for TestElem {
        fn from(value: Vec<u32>) -> Self {
            TestElem::Merged(value.into_boxed_slice())
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct TestString {
        elems: Vec<TestElem>,
    }

    impl From<Vec<TestElem>> for TestString {
        fn from(values: Vec<TestElem>) -> Self {
            TestString { elems: values }
        }
    }

    impl<const N: usize> From<[u32; N]> for TestString {
        fn from(values: [u32; N]) -> Self {
            Self::from(
                values
                    .into_iter()
                    .map(TestElem::Original)
                    .collect::<Vec<_>>(),
            )
        }
    }

    impl TeaString for TestString {
        type Item = TestElem;

        fn len(&self) -> usize {
            self.elems.len()
        }

        fn get(&self, index: usize) -> Option<&Self::Item> {
            self.elems.get(index)
        }

        fn merge_range(&mut self, range: Range<usize>) {
            if range.len() < 2 {
                return;
            }
            let taken = self.elems.drain(range.clone()).collect::<Vec<_>>();
            let mut hashes = Vec::with_capacity(taken.iter().fold(0, |acc, elem| {
                acc + match elem {
                    TestElem::Original(_) => 1,
                    TestElem::Merged(content) => content.len(),
                }
            }));
            for elem in taken {
                match elem {
                    TestElem::Original(hash) => hashes.push(hash),
                    TestElem::Merged(content) => hashes.extend_from_slice(&content),
                }
            }
            self.elems
                .insert(range.start, TestElem::Merged(hashes.into_boxed_slice()));
        }
    }

    #[test]
    fn identical_strings_merge_into_single_element() {
        let mut strings: Vec<TestString> = vec![[0, 1, 2].into(), [0, 1, 2].into()];

        merge_lcs(&mut strings);

        assert_eq!(strings[0], vec![TestElem::from(vec![0, 1, 2])].into());
        assert_eq!(strings[1], vec![TestElem::from(vec![0, 1, 2])].into());
    }

    #[test]
    fn partial_strings_overlap_start_into_two_elements() {
        let mut strings: Vec<TestString> = vec![[0, 1, 2, 3].into(), [0, 1, 2].into()];

        merge_lcs(&mut strings);

        assert_eq!(
            strings[0],
            vec![TestElem::from(vec![0, 1, 2]), TestElem::from(3)].into()
        );
        assert_eq!(strings[1], vec![TestElem::from(vec![0, 1, 2])].into());
    }

    #[test]
    fn partial_strings_overlap_end_into_two_elements() {
        let mut strings: Vec<TestString> = vec![[0, 1, 2, 3].into(), [1, 2, 3].into()];

        merge_lcs(&mut strings);

        assert_eq!(
            strings[0],
            vec![TestElem::from(0), TestElem::from(vec![1, 2, 3])].into()
        );
        assert_eq!(strings[1], vec![TestElem::from(vec![1, 2, 3])].into());
    }

    #[test]
    fn trifecta_multiple_overlap() {
        let mut strings: Vec<TestString> = vec![
           1: [0, 1, 2, 3, 4, 5].into(),
           2: [1, 2, 3, 4, 5].into(),
           3: [2, 3, 4, 5].into(),
        ];

        // 1+2 = [1,2,3,4,5]
        // 1+3: [2,3,4,5]

        merge_lcs(&mut strings);

        assert_eq!(
            strings[0],
            vec![TestElem::from(0), TestElem::from(vec![1, 2, 3, 4, 5]),].into()
        );
        assert_eq!(
            strings[1],
            vec![TestElem::from(vec![1, 2, 3, 4, 5]),].into()
        );
        assert_eq!(strings[2], vec![TestElem::from(vec![2, 3, 4, 5]),].into());
    }
}
*/

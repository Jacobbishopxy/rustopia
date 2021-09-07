//! Iter
//!
//! Iteration control of `Dataframe`.
//! Supporting:
//! 1. iter()
//! 1. into_iter()
//! 1. iter_mut()

use crate::prelude::*;

// TODO: iteration flaw
// 1. vertical orientation (default row orientation)
// 2. `iter_mut` breaks size and other properties

/// iterator returns `Series` (takes ownership)
impl IntoIterator for Dataframe {
    type Item = D1;
    type IntoIter = IntoIteratorDf;

    fn into_iter(self) -> Self::IntoIter {
        IntoIteratorDf {
            iter: self.data.into_iter(),
        }
    }
}

pub struct IntoIteratorDf {
    iter: std::vec::IntoIter<D1>,
}

impl Iterator for IntoIteratorDf {
    type Item = D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&Series`
impl<'a> IntoIterator for &'a Dataframe {
    type Item = &'a D1;
    type IntoIter = IteratorDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IteratorDf {
            iter: self.data.iter(),
        }
    }
}

pub struct IteratorDf<'a> {
    iter: std::slice::Iter<'a, D1>,
}

impl<'a> Iterator for IteratorDf<'a> {
    type Item = &'a D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// iterator returns `&mut Series`
impl<'a> IntoIterator for &'a mut Dataframe {
    type Item = &'a mut D1;
    type IntoIter = IterMutDf<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IterMutDf {
            iter: self.data.iter_mut(),
        }
    }
}

pub struct IterMutDf<'a> {
    iter: std::slice::IterMut<'a, D1>,
}

impl<'a> Iterator for IterMutDf<'a> {
    type Item = &'a mut D1;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// impl `iter` & `iter_mut` methods for `Dataframe`
impl<'a> Dataframe {
    pub fn iter(&'a self) -> IteratorDf<'a> {
        self.into_iter()
    }

    // caution: external mutation cannot update self properties such as `size`
    pub fn iter_mut(&'a mut self) -> IterMutDf<'a> {
        self.into_iter()
    }
}

#[cfg(test)]
mod tiny_df_test {
    use crate::d2;
    use crate::prelude::*;

    #[test]
    fn test_df_iter() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.iter().for_each(|i| {
            println!("{:?}", i);
        });

        // mutate `df`, mocking insert index to each row
        df.iter_mut()
            .enumerate()
            .for_each(|(idx, v)| v.insert(0, DataframeData::Id(idx as u64)));

        println!("{:#?}", df);
    }
}

//! Series constructor

use crate::prelude::*;

fn create_series_index(len: usize) -> Vec<Index> {
    (0..len).map(|i| Index::Id(i as u64)).collect()
}

impl Series {
    /// Create a new series
    pub fn new(name: String, data: D1) -> Self {
        let len = data.len();
        Series {
            name,
            data,
            index: create_series_index(len),
        }
    }

    /// Create a new series from a vector
    /// The 1st element should always be the series name,
    /// otherwise, please use `new`
    pub fn from_vec(data: D1) -> Self {
        let len = data.len();
        if len == 0 {
            return Series::default();
        }

        let mut data = data;
        let name = data.remove(0).to_string();

        Series {
            name,
            data,
            index: create_series_index(len - 1),
        }
    }
}

#[cfg(test)]
mod test_constructor {
    use crate::prelude::*;
    use crate::series;

    #[test]
    fn new1() {
        let s = series!("C1" => ["x", 2, "haha"]);

        println!("{:?}", s);
    }
}

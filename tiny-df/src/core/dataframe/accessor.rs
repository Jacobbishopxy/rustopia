use crate::prelude::*;

// TODO: select row/col, and ranged select, respectively
impl Dataframe {
    /// get data by numbers of index and column
    /// index: row index
    /// column: column index
    pub fn iloc(&self, index: usize, column: usize) -> Option<&DataframeData> {
        match self.data.get(index) {
            Some(r) => match r.get(column) {
                Some(v) => Some(v),
                None => None,
            },
            None => None,
        }
    }

    /// get data by index and column
    /// index: row identity
    /// column: column name
    pub fn loc<I, C>(&self, index: I, column: C) -> Option<&DataframeData>
    where
        I: Into<DataframeData>,
        C: Into<String>,
    {
        let o_i: DataframeData = index.into();
        let o_j: String = column.into();
        let o_i = self.indices.iter().position(|v| v == &o_i);
        let o_j = self.columns_name().iter().position(|c| c == &o_j);

        match self.data_orientation {
            DataOrientation::Horizontal => match o_i {
                Some(i) => {
                    let v = self.data.get(i).unwrap();
                    match o_j {
                        Some(j) => v.get(j),
                        None => None,
                    }
                }
                None => None,
            },
            DataOrientation::Vertical => match o_j {
                Some(j) => {
                    let v = self.data.get(j).unwrap();
                    match o_i {
                        Some(i) => v.get(i),
                        None => todo!(),
                    }
                }
                None => None,
            },
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => None,
        }
    }

    /// get data by a range of column indices
    pub fn icols(&self, _rng0: usize, _rng1: usize) -> Option<&D2> {
        match self.data_orientation {
            DataOrientation::Horizontal => todo!(),
            DataOrientation::Vertical => todo!(),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => todo!(),
        }
    }

    /// get data by a single column name
    pub fn col<S>(&self, _name: S) -> Option<&D1>
    where
        S: Into<String>,
    {
        match self.data_orientation {
            DataOrientation::Horizontal => todo!(),
            DataOrientation::Vertical => todo!(),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => todo!(),
        }
    }

    /// get data by a list of column name
    pub fn cols<S>(&self, _names: Vec<S>) -> Option<&D2>
    where
        S: Into<String>,
    {
        match self.data_orientation {
            DataOrientation::Horizontal => todo!(),
            DataOrientation::Vertical => todo!(),
            #[cfg(feature = "strict")]
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => todo!(),
        }
    }

    /// get dataframe data
    pub fn data(&self) -> &D2 {
        &self.data
    }
}

#[cfg(test)]
mod test_accessor {
    use crate::d2;
    use crate::prelude::*;

    #[test]
    fn test_df_h_iloc_loc() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
            [2, "Mia", "Soft"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        println!("{:?}", df.iloc(1, 2));

        df.replace_indices(&["壹", "贰", "叁"]);

        println!("{:?}", df.loc("叁", "name"));
    }

    #[test]
    fn test_df_v_iloc_loc() {
        let data = d2![
            ["idx", 0, 1, 2],
            ["name", "Jacob", "Sam", "Mia"],
            ["tag", "Cool", "Mellow", "Enthusiastic"],
        ];

        let mut df = Dataframe::from_vec(data, "v");

        println!("{:?}", df.iloc(1, 0));

        df.replace_indices(&["壹", "贰", "叁"]);

        println!("{:?}", df.loc("壹", "tag"));
    }
}

use crate::prelude::*;

// TODO: select row/col, and ranged select, respectively
impl Dataframe {
    /// get data by numbers of index and column
    /// i: row index
    /// j: column index
    pub fn iloc(&self, i: usize, j: usize) -> Option<&DataframeData> {
        match self.data.get(i) {
            Some(r) => match r.get(j) {
                Some(v) => Some(v),
                None => None,
            },
            None => None,
        }
    }

    /// get data by index and column
    /// i: row identity
    /// j: column name
    pub fn loc<T, S>(&self, i: T, j: S) -> Option<&DataframeData>
    where
        T: Into<DataframeData>,
        S: Into<String>,
    {
        let o_i: DataframeData = i.into();
        let o_j: String = j.into();
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
            DataOrientation::Strict => todo!(),
            DataOrientation::Raw => None,
        }
    }

    pub fn icols(&self, rng: (usize, usize)) -> D2 {
        todo!()
    }

    pub fn col<S>(&self, name: S) -> D1
    where
        S: Into<String>,
    {
        todo!()
    }

    pub fn cols<S>(&self, names: Vec<S>) -> D2
    where
        S: Into<String>,
    {
        todo!()
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

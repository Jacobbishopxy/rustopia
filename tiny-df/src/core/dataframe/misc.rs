use crate::prelude::*;

impl Dataframe {
    /// check if input data is empty
    pub fn is_empty(data: &D2) -> bool {
        if data.is_empty() {
            true
        } else {
            data[0].is_empty()
        }
    }

    /// get dataframe sized
    pub fn size(&self) -> (usize, usize) {
        self.size
    }

    /// get dataframe columns
    pub fn columns(&self) -> &Vec<DataframeColumn> {
        &self.columns
    }

    /// get dataframe columns name
    pub fn columns_name(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_owned()).collect()
    }

    pub fn indices(&self) -> &Vec<Index> {
        &self.indices
    }

    /// get dataframe direction
    pub fn data_orientation(&self) -> &DataOrientation {
        &self.data_orientation
    }

    /// rename specific column name
    pub fn rename_column<T>(&mut self, idx: usize, name: T)
    where
        T: Into<String>,
    {
        self.columns.get_mut(idx).map(|c| c.name = name.into());
    }

    /// rename columns
    pub fn rename_columns<T>(&mut self, names: &[T])
    where
        T: Into<String> + Clone,
    {
        self.columns
            .iter_mut()
            .zip(names.iter())
            .for_each(|(c, n)| c.name = n.clone().into())
    }

    /// replace specific index
    pub fn replace_index<T>(&mut self, idx: usize, data: T)
    where
        T: Into<DataframeData>,
    {
        self.indices.get_mut(idx).map(|i| *i = data.into());
    }

    /// replace indices
    pub fn replace_indices<T>(&mut self, indices: &[T])
    where
        T: Into<DataframeData> + Clone,
    {
        self.indices
            .iter_mut()
            .zip(indices.iter())
            .for_each(|(i, r)| *i = r.clone().into())
    }
}

#[cfg(test)]
mod test_misc {
    use crate::d2;
    use crate::prelude::*;

    #[test]
    fn test_df_col_rename() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.rename_column(2, "kind");
        println!("{:#?}", df.columns());

        df.rename_column(5, "OoB");
        println!("{:#?}", df.columns());
    }

    #[test]
    fn test_df_col_renames() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.rename_columns(&["index", "nickname"]);
        println!("{:#?}", df.columns());

        df.rename_columns(&["index", "nickname", "tag", "OoB"]);
        println!("{:#?}", df.columns());
    }

    #[test]
    fn test_df_index_replace() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.replace_index(1, "233");
        println!("{:#?}", df.indices());
    }

    #[test]
    fn test_df_indices_replace() {
        let data = d2![
            ["idx", "name", "tag"],
            [0, "Jacob", "Cool"],
            [1, "Sam", "Mellow"],
        ];

        let mut df = Dataframe::from_vec(data, "h");

        df.replace_indices(&["one"]);
        println!("{:#?}", df.indices());

        df.replace_indices(&["壹", "贰", "叁", "肆"]);
        println!("{:#?}", df.indices());
    }
}

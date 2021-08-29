//! refactor

use serde::{Deserialize, Serialize};

use super::meta::*;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    pub data: D1,
}

impl Series {
    pub fn new(name: String, data: D1) -> Self {
        Series { name, data }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DataFrame {
    pub columns: Vec<String>,
    pub data: D2,
}

impl DataFrame {
    pub fn new(data: Vec<Series>) -> Self {
        let mut columns = vec![];
        let mut d = vec![];

        for i in data.into_iter() {
            columns.push(i.name);
            d.push(i.data);
        }

        DataFrame { columns, data: d }
    }
}

#[cfg(test)]
mod test_rfc {
    use crate::prelude::*;
    use crate::{dfe, ds};

    use super::{DataFrame, Series};

    #[test]
    fn new1() {
        let s = ds!("C1" => ["x", 2, "haha"]);

        println!("{:?}", s);
    }

    #[test]
    fn new2() {
        let s = dfe!(
            "C1" => ["x", 1, "+"],
            "C2" => ["y", 2, "*"],
        );

        println!("{:?}", s);
    }
}

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::core::worksheet::ExcelValue;

#[derive(Debug)]
pub enum DataframeData {
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Error(String),
    None,
    Number(f64),
    String(String),
    Time(NaiveTime),
}

impl<'a> From<ExcelValue<'a>> for DataframeData {
    fn from(ev: ExcelValue<'a>) -> Self {
        match ev {
            ExcelValue::Bool(v) => DataframeData::Bool(v),
            ExcelValue::Date(v) => DataframeData::Date(v),
            ExcelValue::DateTime(v) => DataframeData::DateTime(v),
            ExcelValue::Error(v) => DataframeData::Error(v),
            ExcelValue::None => DataframeData::None,
            ExcelValue::Number(v) => DataframeData::Number(v),
            ExcelValue::String(v) => DataframeData::String(v.to_string()),
            ExcelValue::Time(v) => DataframeData::Time(v),
        }
    }
}

#[derive(Debug)]
pub struct DataframeRow(pub Vec<DataframeData>);

impl DataframeRow {
    fn len(&self) -> usize {
        self.0.len()
    }
}

// TODO: better expression?
#[derive(Debug)]
pub struct Dataframe {
    pub column: Vec<String>,
    pub data: Vec<DataframeRow>,
}

impl Dataframe {
    pub fn new(column: Option<Vec<String>>, data: Vec<DataframeRow>) -> Self {
        match column {
            Some(c) => Self {
                column: c,
                data: data,
            },
            None => {
                let column = (0..data[0].len())
                    .collect::<Vec<usize>>()
                    .iter()
                    .map(|x| x.to_string())
                    .collect();

                Self {
                    column: column,
                    data: data,
                }
            }
        }
    }

    pub fn append(&mut self, _data: DataframeRow) {
        unimplemented!()
    }

    pub fn concat(&mut self, _data: Vec<DataframeRow>) {
        unimplemented!()
    }
}

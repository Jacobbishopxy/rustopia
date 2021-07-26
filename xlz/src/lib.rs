pub mod reader;
pub mod xmlz;

use std::{
    fmt::Display,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use calamine::{open_workbook_auto, CellErrorType, DataType, Range, Reader};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
#[serde(remote = "CellErrorType")]
pub enum DataErrorType {
    Div0,
    NA,
    Name,
    Null,
    Num,
    Ref,
    Value,
    GettingData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Data {
    Empty,
    String(String),
    Float(f64),
    Int(i64),
    DateTime(f64),
    #[serde(with = "DataErrorType")]
    Error(CellErrorType),
    Bool(bool),
}

impl From<DataType> for Data {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Int(v) => Data::Int(v),
            DataType::Float(v) => Data::Float(v),
            DataType::String(v) => Data::String(v),
            DataType::Bool(v) => Data::Bool(v),
            DataType::DateTime(v) => Data::DateTime(v),
            DataType::Error(v) => Data::Error(v),
            DataType::Empty => Data::Empty,
        }
    }
}

fn load(range: &Range<DataType>) -> Vec<Vec<DataType>> {
    let mut res = vec![];

    for r in range.rows() {
        let mut row_cache = vec![];
        for c in r.iter() {
            row_cache.push(c.clone());
        }
        res.push(row_cache);
    }

    res
}

trait JSON {
    fn to_json(&self) -> serde_json::value::Value;
    fn to_string(&self) -> String;
}

impl JSON for Vec<DataType> {
    fn to_json(&self) -> serde_json::Value {
        let foo: Vec<serde_json::Value> = self
            .into_iter()
            .map(|v| json!(&Data::from(v.clone())))
            .collect();
        json!(foo)
    }

    fn to_string(&self) -> String {
        self.to_json().to_string()
    }
}

impl JSON for Vec<Vec<DataType>> {
    fn to_json(&self) -> serde_json::Value {
        let foo: Vec<serde_json::Value> = self.into_iter().map(|v| v.to_json()).collect();
        json!(foo)
    }

    fn to_string(&self) -> String {
        self.to_json().to_string()
    }
}

pub fn range_to_json<W: Write>(dest: &mut W, range: &Range<DataType>) -> std::io::Result<()> {
    let data = load(range);
    let j = serde_json::json!(&data.to_json());

    serde_json::to_writer(dest, &j)?;

    Ok(())
}

pub fn range_to_csv<W: Write>(dest: &mut W, range: &Range<DataType>) -> std::io::Result<()> {
    let n = range.get_size().1 - 1;
    for r in range.rows() {
        for (i, c) in r.iter().enumerate() {
            match *c {
                DataType::Empty => Ok(()),
                DataType::String(ref s) => write!(dest, "{}", s),
                DataType::Float(ref f) | DataType::DateTime(ref f) => write!(dest, "{}", f),
                DataType::Int(ref i) => write!(dest, "{}", i),
                DataType::Error(ref e) => write!(dest, "{:?}", e),
                DataType::Bool(ref b) => write!(dest, "{}", b),
            }?;
            if i != n {
                write!(dest, ";")?;
            }
        }
        write!(dest, "\r\n")?;
    }
    Ok(())
}

pub fn formatted_value(cell: &DataType) {
    match cell {
        DataType::Int(i) => {
            println!("{:?} > int", i)
        }
        DataType::Float(i) => {
            println!("{:?} > float", i)
        }
        DataType::String(i) => {
            println!("{:?} > str", i)
        }
        DataType::Bool(i) => {
            println!("{:?} > bool", i)
        }
        DataType::DateTime(i) => {
            println!("{:?} > datetime", i)
        }
        DataType::Error(i) => {
            println!("{:?} > err", i)
        }
        DataType::Empty => {
            println!("Null > null")
        }
    }
}

pub fn range_to_print(range: &Range<DataType>) {
    for r in range.rows() {
        for c in r.iter() {
            formatted_value(c)
        }
        println!("---");
    }
}

pub fn read_then_print(file: &str, sheet: &str) {
    let sce = PathBuf::from(file);
    match sce.extension().and_then(|s| s.to_str()) {
        Some("xlsx" | "xlsm" | "xlsb" | "xls") => (),
        _ => panic!("Expecting an excel file"),
    }

    let mut xl = open_workbook_auto(&sce).unwrap();
    let range = xl.worksheet_range(&sheet).unwrap().unwrap();

    range_to_print(&range);
}

pub enum WriteExtension {
    JSON,
    CSV,
}

impl Display for WriteExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteExtension::JSON => write!(f, "json"),
            WriteExtension::CSV => write!(f, "csv"),
        }
    }
}

pub fn read_then_write(file: &str, sheet: &str, extension: &WriteExtension) -> std::io::Result<()> {
    let sce = PathBuf::from(file);
    match sce.extension().and_then(|s| s.to_str()) {
        Some("xlsx" | "xlsm" | "xlsb" | "xls") => (),
        _ => panic!("Expecting an excel file"),
    }

    let dest = sce.with_extension(extension.to_string());
    let mut dest = BufWriter::new(File::create(dest).unwrap());

    let mut xl = open_workbook_auto(&sce).unwrap();
    let range = xl.worksheet_range(&sheet).unwrap().unwrap();

    match extension {
        WriteExtension::JSON => range_to_json(&mut dest, &range),
        WriteExtension::CSV => range_to_csv(&mut dest, &range),
    }
}

use std::{borrow::Cow, io::BufReader};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use quick_xml::Reader;
use zip::read::ZipFile;

use super::workbook::{DateSystem, Workbook};

pub struct SheetReader<'a> {
    reader: Reader<BufReader<ZipFile<'a>>>,
    strings: &'a [String],
    styles: &'a [String],
    date_system: &'a DateSystem,
}

impl<'a> SheetReader<'a> {
    /// 创建一个 `SheetReader`
    ///
    /// - `reader` 为一个 reader 对象指向工作簿 xml
    ///
    ///
    ///
    pub fn new(
        reader: Reader<BufReader<ZipFile<'a>>>,
        strings: &'a [String],
        styles: &'a [String],
        date_system: &'a DateSystem,
    ) -> SheetReader<'a> {
        Self {
            reader,
            strings,
            styles,
            date_system,
        }
    }
}

#[derive(Debug)]
pub struct Worksheet {
    pub name: String,
    pub position: u8,
    relationship_id: String,
    target: String,
    sheet_id: u8,
}

impl Worksheet {
    pub fn new(
        relationship_id: String,
        name: String,
        position: u8,
        target: String,
        sheet_id: u8,
    ) -> Self {
        Worksheet {
            name,
            position,
            relationship_id,
            target,
            sheet_id,
        }
    }

    pub fn rows<'a>(&self, workbook: &'a mut Workbook) -> RowIter<'a> {
        let reader = workbook.sheet_reader(&self.target);
        RowIter {
            worksheet_reader: reader,
            want_row: 1,
            next_row: None,
            num_cols: 0,
            num_rows: 0,
            done_file: false,
        }
    }
}

pub struct RowIter<'a> {
    worksheet_reader: SheetReader<'a>,
    want_row: usize,
    next_row: Option<Row<'a>>,
    num_rows: u32,
    num_cols: u16,
    done_file: bool,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Debug)]
pub struct Row<'a>(pub Vec<Cell<'a>>, pub usize);

#[derive(Debug)]
pub struct Cell<'a> {
    pub value: ExcelValue<'a>,
    pub formula: String,
    pub reference: String,
    pub style: String,
    pub cell_type: String,
    pub raw_value: String,
}

#[derive(Debug, PartialEq)]
pub enum ExcelValue<'a> {
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Error(String),
    None,
    Number(f64),
    String(Cow<'a, str>),
    Time(NaiveTime),
}

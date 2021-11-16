use std::cmp;
use std::{borrow::Cow, io::BufReader, mem};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use quick_xml::events::Event;
use quick_xml::Reader;
use zip::read::ZipFile;

use super::{util, DateSystem, Workbook};

/// 用于 `RowIter` 中，为一个 worksheet 导航。其包含一个指向 xlsx 文件中 worksheet `ZipFile` 的指针。
pub struct SheetReader<'a> {
    reader: Reader<BufReader<ZipFile<'a>>>,
    strings: &'a [String],
    styles: &'a [String],
    date_system: &'a DateSystem,
}

impl<'a> SheetReader<'a> {
    /// 创建一个 `SheetReader`
    ///
    /// - `reader` 为一个 reader 对象指向在 zip 文件中的工作簿 xml
    ///
    /// - `strings` 参数需要引用在 xlsx 中使用的字符串向量。背景知识：xlsx 文件不直接存储字符串于每个 spreadsheet 的 xml 文件。
    /// 而是有一个特殊的文件包含所有 workbook 中的字符串。每当一个指定的 worksheet 需要一个字符串，xml 提供该字符串在文件中的索引。
    /// 因此我们需要这个信息用于打印出一个 worksheet 中的任何字符串值。
    ///
    /// - `styles` 用于决定数据类型（主要用于日期）。每个 cell 都有一个 `cell type`。
    ///
    /// - `date_system` 用于决定 date 的类型（起始日期不同，计算方法不同）
    pub(crate) fn new(
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

/// 查询一个指定的 worksheet 使用了多少行和列
fn used_area(used_area_range: &str) -> (u32, u16) {
    let mut end: isize = -1;
    for (i, c) in used_area_range.chars().enumerate() {
        if c == ':' {
            end = i as isize;
            break;
        }
    }
    if end == -1 {
        (0, 0)
    } else {
        let end_range = &used_area_range[end as usize..];
        let mut end = 0;

        for (i, c) in end_range[1..].chars().enumerate() {
            if !c.is_ascii_alphabetic() {
                end = i + 1;
                break;
            }
        }

        let col = util::col2num(&end_range[1..end]).unwrap();
        let row: u32 = end_range[end..].parse().unwrap();
        (row, col)
    }
}

/// 作为本模块的主要对象，存储数据
#[derive(Debug)]
pub struct Worksheet {
    pub name: String,
    pub position: u8,
    relationship_id: String,
    target: String,
    sheet_id: u8,
}

impl Worksheet {
    /// worksheet 构造函数。注意本方法不应该被直接调用，而是从 `Workbook` 对象中使用
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

    /// 获取本 worksheet 的一个 `RowIter`。本库最重要的部分。使用本方法遍历 sheet 的所有值。
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

    pub fn relationship_id(&self) -> &str {
        &self.relationship_id
    }

    pub fn sheet_id(&self) -> u8 {
        self.sheet_id
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
        // xlsx 文件的 xml 中空行不会保存空元素。
        // 因此我们需要模拟空行因为用户期望在遍历时见到它。
        if let Some(Row(_, row_num)) = &self.next_row {
            // 由于我们现在在缓存一个行，最终的结果是返回它或是模拟该行。
            // 因此我们需要当前行并更新需要新的一行。
            // 接着我们需要知道该行是否所需，或是使其为空
            let current_row = self.want_row;
            self.want_row += 1;
            if *row_num == current_row {
                // 遍历结束
                let mut r = None;
                mem::swap(&mut r, &mut self.next_row);
                return r;
            } else {
                return empty_row(self.num_cols, current_row);
            }
        } else if self.done_file && self.want_row < self.num_rows as usize {
            self.want_row += 1;
            return empty_row(self.num_cols, self.want_row - 1);
        }

        let mut buf = Vec::new();
        let reader = &mut self.worksheet_reader.reader;
        let strings = self.worksheet_reader.strings;
        let styles = self.worksheet_reader.styles;
        let date_system = self.worksheet_reader.date_system;
        let next_row = {
            let mut row: Vec<Cell> = Vec::with_capacity(self.num_cols as usize);
            let mut in_cell = false;
            let mut in_value = false;
            let mut c = new_cell();
            let mut this_row: usize = 0;
            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Empty(ref e)) if e.name() == b"dimension" => {
                        if let Some(used_area_range) = util::get(e.attributes(), b"ref") {
                            if used_area_range != "A1" {
                                let (rows, cols) = used_area(&used_area_range);
                                self.num_cols = cols;
                                self.num_rows = rows;
                            }
                        }
                    }
                    Ok(Event::Start(ref e)) if e.name() == b"row" => {
                        this_row = util::get(e.attributes(), b"r").unwrap().parse().unwrap();
                    }
                    Ok(Event::Start(ref e)) if e.name() == b"c" => {
                        in_cell = true;
                        e.attributes().for_each(|a| {
                            let a = a.unwrap();
                            if a.key == b"r" {
                                c.reference = util::attr_value(&a);
                            }
                            if a.key == b"t" {
                                c.cell_type = util::attr_value(&a);
                            }
                            if a.key == b"s" {
                                if let Ok(num) = util::attr_value(&a).parse::<usize>() {
                                    if let Some(style) = styles.get(num) {
                                        c.style = style.to_string();
                                    }
                                }
                            }
                        });
                    }
                    Ok(Event::Start(ref e)) if e.name() == b"v" => {
                        in_value = true;
                    }
                    // 注意：因为 v 元素是 c 元素的子元素，需要在 `in_cell` 检查前完成
                    Ok(Event::Text(ref e)) if in_value => {
                        c.raw_value = e.unescape_and_decode(&reader).unwrap();
                        c.value = match &c.cell_type[..] {
                            "s" => {
                                if let Ok(pos) = c.raw_value.parse::<usize>() {
                                    let s = &strings[pos];
                                    ExcelValue::String(Cow::Borrowed(s))
                                } else {
                                    ExcelValue::String(Cow::Owned(c.raw_value.clone()))
                                }
                            }
                            "str" => ExcelValue::String(Cow::Owned(c.raw_value.clone())),
                            "b" => {
                                if c.raw_value == "0" {
                                    ExcelValue::Bool(false)
                                } else {
                                    ExcelValue::Bool(true)
                                }
                            }
                            "bl" => ExcelValue::None,
                            "e" => ExcelValue::Error(c.raw_value.to_string()),
                            _ if is_date(&c) => {
                                let num = c.raw_value.parse::<f64>().unwrap();
                                match util::excel_number_to_date(num, date_system) {
                                    util::DateConversion::Date(date) => ExcelValue::Date(date),
                                    util::DateConversion::DateTime(date) => {
                                        ExcelValue::DateTime(date)
                                    }
                                    util::DateConversion::Time(time) => ExcelValue::Time(time),
                                    util::DateConversion::Number(num) => {
                                        ExcelValue::Number(num as f64)
                                    }
                                }
                            }
                            _ => ExcelValue::Number(c.raw_value.parse::<f64>().unwrap()),
                        }
                    }
                    Ok(Event::Text(ref e)) if in_cell => {
                        let txt = e.unescape_and_decode(&reader).unwrap();
                        c.formula.push_str(&txt)
                    }
                    Ok(Event::End(ref e)) if e.name() == b"v" => {
                        in_value = false;
                    }
                    Ok(Event::End(ref e)) if e.name() == b"c" => {
                        if let Some(prev) = row.last() {
                            let (mut last_col, _) = prev.coordinates();
                            let (this_col, this_row) = c.coordinates();
                            while this_col > last_col + 1 {
                                let mut cell = new_cell();
                                cell.reference
                                    .push_str(&util::num2col(last_col + 1).unwrap());
                                cell.reference.push_str(&this_row.to_string());
                                row.push(cell);
                                last_col += 1;
                            }
                            row.push(c);
                        } else {
                            let (this_col, this_row) = c.coordinates();
                            for n in 1..this_col {
                                let mut cell = new_cell();
                                cell.reference.push_str(&util::num2col(n).unwrap());
                                cell.reference.push_str(&this_row.to_string());
                                row.push(cell);
                            }
                            row.push(c);
                        }
                        c = new_cell();
                        in_cell = false;
                    }
                    Ok(Event::End(ref e)) if e.name() == b"row" => {
                        self.num_cols = cmp::max(self.num_cols, row.len() as u16);
                        while row.len() < self.num_cols as usize {
                            let mut cell = new_cell();
                            cell.reference
                                .push_str(&util::num2col(row.len() as u16 + 1).unwrap());
                            cell.reference.push_str(&this_row.to_string());
                            row.push(cell);
                        }
                        let next_row = Some(Row(row, this_row));
                        if this_row == self.want_row {
                            break next_row;
                        } else {
                            self.next_row = next_row;
                            break empty_row(self.num_cols, self.want_row);
                        }
                    }
                    Ok(Event::Eof) => break None,
                    Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                    _ => (),
                }
                buf.clear();
            }
        };
        self.want_row += 1;
        if next_row.is_none() && self.want_row - 1 < self.num_rows as usize {
            self.done_file = true;
            return empty_row(self.num_cols, self.want_row - 1);
        }
        next_row
    }
}

fn new_cell() -> Cell<'static> {
    Cell {
        value: ExcelValue::None,
        formula: "".to_string(),
        reference: "".to_string(),
        style: "".to_string(),
        cell_type: "".to_string(),
        raw_value: "".to_string(),
    }
}

fn empty_row(num_cols: u16, this_row: usize) -> Option<Row<'static>> {
    let mut row = vec![];
    for n in 0..num_cols {
        let mut c = new_cell();
        c.reference.push_str(&util::num2col(n + 1).unwrap());
        c.reference.push_str(&this_row.to_string());
        row.push(c);
    }
    Some(Row(row, this_row))
}

fn is_date(cell: &Cell) -> bool {
    let is_d = cell.style == "d";
    let is_like_d_and_not_like_red = cell.style.contains('d') && !cell.style.contains("Red");
    let is_like_m = cell.style.contains('m');
    if is_d || is_like_d_and_not_like_red || is_like_m {
        true
    } else {
        cell.style.contains('y')
    }
}

#[derive(Debug)]
pub struct Row<'a>(pub Vec<Cell<'a>>, pub usize);

#[derive(Debug)]
pub struct Cell<'a> {
    /// 转换原始值为 Rust 中的值
    pub value: ExcelValue<'a>,
    /// 公式
    pub formula: String,
    /// 单元的引用，例如 B3, A1 等
    pub reference: String,
    /// 单元的样式
    pub style: String,
    /// 由 Excel 所记录的单元类型
    pub cell_type: String,
    /// xml 中记录的原始数据
    pub raw_value: String,
}

impl Cell<'_> {
    /// 返回当前 cell 的 row/column 定位
    pub fn coordinates(&self) -> (u16, u32) {
        let (col, row) = {
            let r = &self.reference;
            let mut end = 0;
            for (i, c) in r.chars().enumerate() {
                if !c.is_ascii_alphabetic() {
                    end = i;
                    break;
                }
            }
            (&r[..end], &r[end..])
        };
        let col = util::col2num(col).unwrap();
        let row = row.parse().unwrap();
        (col, row)
    }
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

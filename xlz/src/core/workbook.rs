use std::{collections::HashMap, fs::File, io::BufReader};

use quick_xml::{events::Event, Reader};
use zip::ZipArchive;

use crate::core::utils;
use crate::core::worksheet::{SheetReader, Worksheet};

#[derive(Debug)]
pub enum DateSystem {
    V1900,
    V1904,
}

#[derive(Debug)]
pub struct Workbook {
    // pub path: String,
    xls: ZipArchive<File>,
    encoding: String,
    pub date_system: DateSystem,
    strings: Vec<String>,
    styles: Vec<String>,
}

#[derive(Debug)]
pub struct SheetMap {
    sheets_by_name: HashMap<String, u8>,
    sheets_by_num: Vec<Option<Worksheet>>,
}

impl SheetMap {
    pub fn by_name(&self) -> Vec<&str> {
        self.sheets_by_num
            .iter()
            .filter(|&s| s.is_some())
            .map(|s| &s.as_ref().unwrap().name[..])
            .collect()
    }
}

pub enum SheetNameOrNum<'a> {
    Name(&'a str),
    Pos(usize),
}

pub trait SheetAccessTrait {
    fn go(&self) -> SheetNameOrNum;
}

impl SheetAccessTrait for &str {
    fn go(&self) -> SheetNameOrNum {
        SheetNameOrNum::Name(*self)
    }
}

impl SheetAccessTrait for usize {
    fn go(&self) -> SheetNameOrNum {
        SheetNameOrNum::Pos(*self)
    }
}

impl SheetMap {
    pub fn get<T: SheetAccessTrait>(&self, sheet: T) -> Option<&Worksheet> {
        let sheet = sheet.go();
        match sheet {
            SheetNameOrNum::Name(n) => match self.sheets_by_name.get(n) {
                Some(p) => self.sheets_by_num.get(*p as usize)?.as_ref(),
                None => None,
            },
            SheetNameOrNum::Pos(n) => self.sheets_by_num.get(n)?.as_ref(),
        }
    }

    pub fn len(&self) -> u8 {
        (self.sheets_by_num.len() - 1) as u8
    }
}

impl Workbook {
    /// xlsx zips 包含了一个带有 “ids” 至 “targets” 映射的 xml 文件。
    /// ids 用于鉴别文件中的工作簿，而 targets 则拥有如何在 zip 中寻找工作簿的信息。
    /// 本函数返回一个 id -> target 的 hashmap，这样你可以快速的判定 zip 中 xml 文件的工作簿名称。
    fn rels(&mut self) -> HashMap<String, String> {
        let mut map = HashMap::new();

        match self.xls.by_name("xl/_rels/workbook.xml.rels") {
            Ok(rels) => {
                // 可以打印 xml 结构
                // let _ = std::io::copy(&mut rels, &mut std::io::stdout());

                let reader = BufReader::new(rels);
                let mut reader = Reader::from_reader(reader);
                reader.trim_text(true);

                let mut buf = Vec::new();
                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Empty(ref e)) if e.name() == b"Relationship" => {
                            let mut id = String::new();
                            let mut target = String::new();
                            e.attributes().for_each(|a| {
                                let a = a.unwrap();
                                if a.key == b"Id" {
                                    id = utils::attr_value(&a);
                                }
                                if a.key == b"Target" {
                                    target = utils::attr_value(&a);
                                }
                            });
                            map.insert(id, target);
                        }
                        Ok(Event::Eof) => break,
                        Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                        _ => (),
                    }
                    buf.clear();
                }

                map
            }
            Err(_) => map,
        }
    }

    /// 返回 `SheetMap` 包含本 workbook 中的所有工作簿
    pub fn sheets(&mut self) -> SheetMap {
        let rels = self.rels();
        let num_sheets = rels
            .iter()
            .filter(|(_, v)| v.starts_with("worksheet"))
            .count();
        let mut sheets = SheetMap {
            sheets_by_name: HashMap::new(),
            sheets_by_num: Vec::with_capacity(num_sheets + 1),
        };
        sheets.sheets_by_num.push(None);

        match self.xls.by_name("xl/workbook.xml") {
            Ok(wb) => {
                // let _ = std::io::copy(&mut wb, &mut std::io::stdout());
                let reader = BufReader::new(wb);
                let mut reader = Reader::from_reader(reader);
                reader.trim_text(true);

                let mut buf = Vec::new();
                let mut current_sheet_num: u8 = 0;

                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Empty(ref e)) if e.name() == b"sheet" => {
                            current_sheet_num += 1;
                            let mut name = String::new();
                            let mut id = String::new();
                            let mut num = 0;
                            e.attributes().for_each(|a| {
                                let a = a.unwrap();
                                if a.key == b"r:id" {
                                    id = utils::attr_value(&a);
                                }
                                if a.key == b"name" {
                                    name = utils::attr_value(&a);
                                }
                                if a.key == b"sheetId" {
                                    if let Ok(r) = utils::attr_value(&a).parse() {
                                        num = r;
                                    }
                                }
                            });
                            sheets
                                .sheets_by_name
                                .insert(name.clone(), current_sheet_num);
                            let target = {
                                let s = rels.get(&id).unwrap();
                                if let Some(stripped) = s.strip_prefix('/') {
                                    stripped.to_string()
                                } else {
                                    "xl/".to_owned() + s
                                }
                            };
                            let ws = Worksheet::new(id, name, current_sheet_num, target, num);
                            sheets.sheets_by_num.push(Some(ws));
                        }
                        Ok(Event::Eof) => break,
                        Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                        _ => (),
                    }
                    buf.clear();
                }
                sheets
            }
            Err(_) => todo!(),
        }
    }

    pub fn new(file: File) -> Result<Self, String> {
        match ZipArchive::new(file) {
            Ok(mut xls) => {
                let strings = strings(&mut xls);
                let styles = find_styles(&mut xls);
                let date_system = get_date_system(&mut xls);
                Ok(Workbook {
                    xls,
                    encoding: String::from("utf8"),
                    date_system,
                    strings,
                    styles,
                })
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// 打印所有 xlsx zip 中的内部文件
    pub fn contents(&mut self) {
        unimplemented!()
    }

    /// 为指定的 worksheet 创建一个 SheetReader （用于遍历所有行，等等）
    pub fn sheet_reader<'a>(&'a mut self, zip_target: &str) -> SheetReader<'a> {
        let target = match self.xls.by_name(zip_target) {
            Ok(ws) => ws,
            Err(_) => panic!("Could not find worksheet: {}", zip_target),
        };
        // let _ = std::io::copy(&mut target, &mut std::io::stdout());

        let reader = BufReader::new(target);
        let mut reader = Reader::from_reader(reader);
        reader.trim_text(true);
        SheetReader::new(reader, &self.strings, &self.styles, &self.date_system)
    }
}

fn strings(zip_file: &mut ZipArchive<File>) -> Vec<String> {
    let mut strings = Vec::new();
    match zip_file.by_name("xl/sharedStrings.xml") {
        Ok(strings_file) => {
            let reader = BufReader::new(strings_file);
            let mut reader = Reader::from_reader(reader);
            reader.trim_text(true);
            let mut buf = Vec::new();
            let mut this_string = String::new();
            let mut preserve_space = false;

            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Start(ref e)) if e.name() == b"t" => {
                        if let Some(att) = utils::get(e.attributes(), b"xml:space") {
                            if att == "preserve" {
                                preserve_space = true;
                            } else {
                                preserve_space = false;
                            }
                        } else {
                            preserve_space = false;
                        }
                    }
                    Ok(Event::Text(ref e)) => {
                        this_string.push_str(&e.unescape_and_decode(&reader).unwrap()[..])
                    }
                    Ok(Event::Empty(ref e)) if e.name() == b"t" => strings.push("".to_owned()),
                    Ok(Event::End(ref e)) if e.name() == b"t" => {
                        if preserve_space {
                            strings.push(this_string.to_owned());
                        } else {
                            strings.push(this_string.trim().to_owned());
                        }
                        this_string = String::new();
                    }
                    Ok(Event::Eof) => break,
                    Err(_) => todo!(),
                    _ => (),
                }
                buf.clear();
            }
            strings
        }
        Err(_) => strings,
    }
}

/// 查询 worksheet 特定位置的行列样式
fn find_styles(xlsx: &mut ZipArchive<File>) -> Vec<String> {
    let mut styles = Vec::new();
    let mut number_formats = standard_styles();
    let styles_xml = match xlsx.by_name("xl/styles.xml") {
        Ok(s) => s,
        Err(_) => return styles,
    };
    // let _ = std::io::copy(&mut styles_xml, &mut std::io::stdout());

    let reader = BufReader::new(styles_xml);
    let mut reader = Reader::from_reader(reader);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut record_styles = false;
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Empty(ref e)) if e.name() == b"numFmt" => {
                let id = utils::get(e.attributes(), b"numFmtId").unwrap();
                let code = utils::get(e.attributes(), b"formatCode").unwrap();
                number_formats.insert(id, code);
            }
            Ok(Event::Start(ref e)) if e.name() == b"cellXfs" => {
                // Section 2.1.589 Part 1 Section 18.3.1.4, c (Cell)
                record_styles = true;
            }
            Ok(Event::End(ref e)) if e.name() == b"cellXfs" => record_styles = false,
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e))
                if record_styles && e.name() == b"xf" =>
            {
                let id = utils::get(e.attributes(), b"numFmtId").unwrap();
                if number_formats.contains_key(&id) {
                    styles.push(number_formats.get(&id).unwrap().to_string());
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        buf.clear();
    }
    styles
}

/// 标准样式 (ISO/IEC 29500:2011 in Part 1, section 18.8.30)
fn standard_styles() -> HashMap<String, String> {
    let mut styles = HashMap::new();
    let standard_styles = [
        ["0", "General"],
        ["1", "0"],
        ["2", "0.00"],
        ["3", "#,##0"],
        ["4", "#,##0.00"],
        ["9", "0%"],
        ["10", "0.00%"],
        ["11", "0.00E+00"],
        ["12", "# ?/?"],
        ["13", "# ??/??"],
        ["14", "mm-dd-yy"],
        ["15", "d-mmm-yy"],
        ["16", "d-mmm"],
        ["17", "mmm-yy"],
        ["18", "h:mm AM/PM"],
        ["19", "h:mm:ss AM/PM"],
        ["20", "h:mm"],
        ["21", "h:mm:ss"],
        ["22", "m/d/yy h:mm"],
        ["37", "#,##0 ;(#,##0)"],
        ["38", "#,##0 ;[Red](#,##0)"],
        ["39", "#,##0.00;(#,##0.00)"],
        ["40", "#,##0.00;[Red](#,##0.00)"],
        ["45", "mm:ss"],
        ["46", "[h]:mm:ss"],
        ["47", "mmss.0"],
        ["48", "##0.0E+0"],
        ["49", "@"],
    ];
    for style in standard_styles {
        let [id, code] = style;
        styles.insert(id.to_string(), code.to_string());
    }
    styles
}

fn get_date_system(xlsx: &mut ZipArchive<File>) -> DateSystem {
    match xlsx.by_name("xl/workbook.xml") {
        Ok(wb) => {
            let reader = BufReader::new(wb);
            let mut reader = Reader::from_reader(reader);
            reader.trim_text(true);
            let mut buf = Vec::new();
            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Empty(ref e)) if e.name() == b"workbookPr" => {
                        if let Some(system) = utils::get(e.attributes(), b"date1904") {
                            if system == "1" {
                                break DateSystem::V1904;
                            }
                        }
                        break DateSystem::V1900;
                    }
                    Ok(Event::Eof) => break DateSystem::V1900,
                    Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                    _ => (),
                }
                buf.clear();
            }
        }
        Err(_) => panic!("Could not find xl/workbook.xml"),
    }
}

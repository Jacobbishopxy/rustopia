use std::{fs::File, path::Path};

use crate::Workbook;

#[derive(Debug)]
pub enum Source {
    File(File),
    Path(String),
    Uri(String),
}

pub fn from_file(file: File) -> Result<Workbook, String> {
    Workbook::new(file)
}

pub fn from_path(path: &str) -> Result<Workbook, String> {
    if !Path::new(&path).exists() {
        let err = format!("'{}' does not exist", &path);
        return Err(err);
    }
    let zip_file = match File::open(&path) {
        Ok(z) => z,
        Err(e) => return Err(e.to_string()),
    };
    Workbook::new(zip_file)
}

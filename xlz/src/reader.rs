use std::{fs::File, path::Path};

use crate::Workbook;

#[derive(Debug)]
pub enum Source<'a> {
    File(File),
    Path(&'a str),
    #[cfg(feature = "rqw")]
    Url(&'a str),
}

impl<'a> Source<'a> {
    pub fn read(self) -> Result<Workbook, String> {
        match self {
            Source::File(f) => from_file(f),
            Source::Path(s) => from_path(s),
            #[cfg(feature = "rqw")]
            Source::Url(s) => from_url(s),
        }
    }
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

#[cfg(feature = "rqw")]
pub fn from_url(path: &str) -> Result<Workbook, String> {
    match reqwest::blocking::get(path) {
        Ok(r) => {
            if !r.status().is_success() {
                let mut res = r;
                let mut file = File::create("tmp").unwrap();
                std::io::copy(&mut res, &mut file).unwrap();
                Workbook::new(file)
            } else {
                Err("url unable to open".to_owned())
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

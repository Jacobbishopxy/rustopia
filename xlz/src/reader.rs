//! Reader

use std::fs::File;

use crate::{Workbook, XlzResult};

#[derive(Debug)]
pub enum Source<'a> {
    File(File),
    Path(&'a str),
    #[cfg(feature = "rqw")]
    Url(&'a str),
}

impl<'a> Source<'a> {
    pub fn read(self) -> XlzResult<Workbook> {
        match self {
            Source::File(f) => from_file(f),
            Source::Path(s) => from_path(s),
            #[cfg(feature = "rqw")]
            Source::Url(s) => from_url(s),
        }
    }
}

pub fn from_file(file: File) -> XlzResult<Workbook> {
    Workbook::new(file)
}

pub fn from_path(path: &str) -> XlzResult<Workbook> {
    let zip_file = File::open(&path)?;
    Workbook::new(zip_file)
}

#[cfg(feature = "rqw")]
pub fn from_url(path: &str) -> XlzResult<Workbook> {
    use crate::XlzError;

    match reqwest::blocking::get(path) {
        Ok(r) => {
            if !r.status().is_success() {
                let mut res = r;
                let mut file = File::create("tmp").unwrap();
                std::io::copy(&mut res, &mut file)?;
                Workbook::new(file)
            } else {
                Err(XlzError::CommonError(format!("unable to open {:?}", path)))
            }
        }
        Err(e) => Err(XlzError::CommonError(format!(
            "reqwset error {:?}",
            e.to_string()
        ))),
    }
}

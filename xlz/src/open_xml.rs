use std::{
    cell::RefCell,
    fs::File,
    io::{Cursor, Read, Seek},
    path::{Path, PathBuf},
    rc::Rc,
};

use derivative::Derivative;
use linked_hash_map::LinkedHashMap;
use thiserror::Error;
use zip::ZipArchive;

pub type OXResult<T> = std::result::Result<T, OpenXmlErr>;

#[derive(Error, Debug)]
pub enum OpenXmlErr {
    #[error("zip error")]
    ZipError(#[from] zip::result::ZipError),
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("xml error")]
    XmlError(#[from] quick_xml::Error),
    #[error("xml deserialization error")]
    XmlDeError(#[from] quick_xml::de::DeError),
    #[error("No content type in package")]
    PackageContentTypeError,
    #[error("unknown data store error")]
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct OpenXmlPrt {
    uri: PathBuf,
    raw: Cursor<Vec<u8>>,
}

impl OpenXmlPrt {
    pub fn from_reader<S: Into<PathBuf>, R: Read>(
        uri: S,
        mut reader: R,
    ) -> Result<Self, OpenXmlErr> {
        let mut raw = Cursor::new(Vec::new());
        std::io::copy(&mut reader, &mut raw)?;
        let prt = Self {
            uri: uri.into(),
            ..Default::default()
        };

        Ok(prt)
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpenXmlPkg {
    parts: LinkedHashMap<String, OpenXmlPrt>,
}

impl OpenXmlPkg {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, OpenXmlErr> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }

    pub fn from_reader<R: Read + Seek>(reader: R) -> Result<Self, OpenXmlErr> {
        let mut zip = ZipArchive::new(reader)?;
        let mut pkg = OpenXmlPkg::default();

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;

            if file.is_dir() {
                continue;
            }

            let filename = file.name().to_string();

            let uri = PathBuf::from(&filename);
            let prt = OpenXmlPrt::from_reader(uri, &mut file)?;

            pkg.parts.insert(filename, prt);
        }

        Ok(pkg)
    }
}

#[derive(Derivative, Default)]
#[derivative(Debug)]
pub struct SpreadsheetDoc {
    #[derivative(Debug = "ignore")]
    pkg: Rc<RefCell<OpenXmlPkg>>,
    // parts: Rc<RefCell<SpreadsheetPrts>>,
    // workbook: Workbook, // TODO: implement
}

impl SpreadsheetDoc {
    pub fn open<P: AsRef<Path>>(path: P) -> OXResult<Self> {
        let pkg = Rc::new(RefCell::new(OpenXmlPkg::open(path)?));

        Ok(Self { pkg: pkg })
    }
}

#[test]
fn test_open_xlsx() {
    let pkg = SpreadsheetDoc::open("test.xlsx").unwrap();

    println!("{:?}", pkg);
}

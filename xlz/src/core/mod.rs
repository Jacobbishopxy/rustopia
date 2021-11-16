//! Core

mod util;
pub mod workbook;
pub(crate) mod worksheet;

pub use workbook::Workbook;
pub(crate) use worksheet::{SheetReader, Worksheet};

#[derive(Debug)]
pub(crate) enum DateSystem {
    V1900,
    V1904,
}

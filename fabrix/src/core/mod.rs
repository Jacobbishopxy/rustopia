//! Fabrix core

pub mod dataframe;
pub mod row;
pub mod series;
pub mod util;
pub mod value;

pub use dataframe::*;
pub use row::*;
pub use series::*;
pub use value::*;

use polars::prelude::{DataType, Field};

pub use util::IDX;
pub(crate) use util::{cis_err, inf_err, oob_err, Stepper};

/// field info: column name, column type & has null
#[derive(Debug, Clone, PartialEq)]
pub struct FieldInfo {
    pub(crate) field: Field,
    pub(crate) has_null: bool,
}

impl FieldInfo {
    pub fn new(field: Field, has_null: bool) -> Self {
        FieldInfo { field, has_null }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &String {
        &self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.data_type()
    }

    pub fn has_null(&self) -> bool {
        self.has_null
    }
}

impl From<Field> for FieldInfo {
    fn from(f: Field) -> Self {
        FieldInfo::new(f, true)
    }
}

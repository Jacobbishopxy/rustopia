//! Sql Builder: ddl mutation

use polars::prelude::DataType;
use sea_query::{ColumnDef, Table};

use super::{alias, statement};
use crate::{adt, DdlMutation, FieldInfo, SqlBuilder};

impl DdlMutation for SqlBuilder {
    /// given a `Dataframe` columns, generate SQL create_table string
    fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<FieldInfo>,
        index_option: Option<&adt::IndexOption>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(alias!(table_name)).if_not_exists();

        if let Some(idx) = index_option {
            statement.col(&mut gen_primary_col(idx));
        }

        columns.iter().for_each(|c| {
            statement.col(&mut gen_col(c));
        });

        statement!(self, statement)
    }

    /// drop a table by its name
    fn delete_table(&self, table_name: &str) -> String {
        let mut statement = Table::drop();
        statement.table(alias!(table_name));

        statement!(self, statement)
    }
}

/// generate a primary column
fn gen_primary_col(index_option: &adt::IndexOption) -> ColumnDef {
    let mut cd = ColumnDef::new(alias!(index_option.name));

    match index_option.index_type {
        adt::IndexType::Int => cd.integer(),
        adt::IndexType::BigInt => cd.big_integer(),
        adt::IndexType::Uuid => cd.uuid(),
    };

    cd.not_null().auto_increment().primary_key();

    cd
}

/// generate column by `DataframeColumn`
fn gen_col(field: &FieldInfo) -> ColumnDef {
    let mut c = ColumnDef::new(alias!(field.name()));
    match field.data_type() {
        DataType::Boolean => c.boolean(),
        DataType::UInt8 => c.integer(),
        DataType::UInt16 => c.integer(),
        DataType::UInt32 => c.integer(),
        DataType::UInt64 => c.big_integer(),
        DataType::Int8 => c.integer(),
        DataType::Int16 => c.integer(),
        DataType::Int32 => c.integer(),
        DataType::Int64 => c.big_integer(),
        DataType::Float32 => c.double(),
        DataType::Float64 => c.float(),
        DataType::Utf8 => c.string(),
        DataType::Object("Date") => c.date(),
        DataType::Object("Time") => c.time(),
        DataType::Object("DateTime") => c.date_time(),
        DataType::Object("Uuid") => c.uuid(),
        DataType::Object("Decimal") => c.decimal(),
        _ => unimplemented!(),
    };

    if !field.has_null {
        c.not_null();
    }

    c
}

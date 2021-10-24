use polars::prelude::DataType;
use sea_query::{Alias, ColumnDef, Table};

use super::super::{statement, IndexOption, IndexType, TableField};
use crate::{DdlMutation, SqlBuilder};

impl DdlMutation for SqlBuilder {
    /// given a `Dataframe` columns, generate SQL create_table string
    fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<TableField>,
        index_option: Option<&IndexOption>,
    ) -> String {
        let mut statement = Table::create();
        statement.table(Alias::new(table_name)).if_not_exists();

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
        statement.table(Alias::new(table_name));

        statement!(self, statement)
    }
}

/// generate a primary column
fn gen_primary_col(index_option: &IndexOption) -> ColumnDef {
    let mut cd = ColumnDef::new(Alias::new(index_option.name));

    match index_option.index_type {
        IndexType::Int => cd.integer(),
        IndexType::BigInt => cd.big_integer(),
        IndexType::Uuid => cd.uuid(),
    };

    cd.not_null().auto_increment().primary_key();

    cd
}

/// generate column by `DataframeColumn`
fn gen_col(field: &TableField) -> ColumnDef {
    let mut c = ColumnDef::new(Alias::new(field.name()));
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
        DataType::Date32 => c.date_time(),
        DataType::Date64 => c.date_time(),
        DataType::Time64(_) => c.time(),
        _ => unimplemented!(),
    };

    if !field.nullable {
        c.not_null();
    }

    c
}

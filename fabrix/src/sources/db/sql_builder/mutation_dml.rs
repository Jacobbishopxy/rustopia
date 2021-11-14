//! Sql Builder: dml mutation

use sea_query::{Expr, Query};

use super::{alias, filter_builder, statement, try_from_value_to_svalue, DeleteOrSelect};
use crate::{adt, DataFrame, DmlMutation, FabrixResult, SqlBuilder};

impl DmlMutation for SqlBuilder {
    /// given a `Dataframe`, insert it into an existing table
    fn insert(&self, table_name: &str, df: DataFrame, ignore_index: bool) -> FabrixResult<String> {
        // announce an insert statement
        let mut statement = Query::insert();
        // given a table name, insert into it
        statement.into_table(alias!(table_name));
        // if the index is not ignored, insert as the primary key
        if !ignore_index {
            statement.columns(vec![alias!(df.index.name())]);
        }
        // the rest of the dataframe's columns
        statement.columns(df.fields().iter().map(|c| alias!(&c.name)));

        let column_info = df.fields();
        for c in df.into_iter() {
            let record = c
                .data
                .into_iter()
                .zip(column_info.iter())
                .map(|(v, inf)| try_from_value_to_svalue(v, &inf.dtype(), true))
                .collect::<FabrixResult<Vec<_>>>()?;

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, update to an existing table in terms of df index
    fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &adt::IndexOption,
    ) -> FabrixResult<Vec<String>> {
        let column_info = df.fields();
        let indices_type = df.index_dtype().clone();
        let mut res = vec![];

        for row in df.into_iter() {
            let mut statement = Query::update();
            statement.table(alias!(table_name));

            let itr = row.data.into_iter().zip(column_info.iter());
            let mut updates = vec![];

            for (v, inf) in itr {
                let alias = alias!(&inf.name);
                let svalue = try_from_value_to_svalue(v, &inf.dtype(), true)?;
                updates.push((alias, svalue));
            }

            statement.values(updates).and_where(
                Expr::col(alias!(&index_option.name)).eq(try_from_value_to_svalue(
                    row.index,
                    &indices_type,
                    false,
                )?),
            );

            statement!(res; self, statement)
        }

        Ok(res)
    }

    /// delete from an existing table
    fn delete(&self, delete: &adt::Delete) -> String {
        let mut statement = Query::delete();
        statement.from_table(alias!(&delete.table));

        filter_builder(&mut DeleteOrSelect::Delete(&mut statement), &delete.filter);

        statement!(self, statement)
    }
}

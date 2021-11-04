//! Fabrix db sql_builder dml mutation

use sea_query::{Cond, Expr, Query};

use super::{alias, statement, try_from_value_to_svalue};
use crate::{adt, DataFrame, DmlMutation, FabrixResult, Series, SqlBuilder};

impl DmlMutation for SqlBuilder {
    /// given a `Dataframe`, insert it into an existing table
    fn insert(&self, table_name: &str, df: DataFrame) -> FabrixResult<String> {
        // announce an insert statement
        let mut statement = Query::insert();
        // given a table name, insert into it
        statement.into_table(alias!(table_name));
        // dataframe's index is always the primary key
        statement.columns(vec![alias!(df.index.name())]);
        // the rest of the dataframe's columns
        statement.columns(df.fields().iter().map(|c| alias!(c.name())));

        let column_info = df.fields();
        for c in df.into_iter() {
            let record = c
                .data
                .into_iter()
                .zip(column_info.iter())
                .map(|(v, inf)| try_from_value_to_svalue(v, inf.data_type(), inf.has_null()))
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
                let alias = alias!(inf.name());
                let svalue = try_from_value_to_svalue(v, inf.data_type(), inf.has_null())?;
                updates.push((alias, svalue));
            }

            statement.values(updates).and_where(
                Expr::col(alias!(index_option.name)).eq(try_from_value_to_svalue(
                    row.index,
                    &indices_type,
                    false,
                )?),
            );

            statement!(res; self, statement)
        }

        Ok(res)
    }

    fn delete(&self, table_name: &str, index: Series) -> FabrixResult<String> {
        let mut statement = Query::delete();
        statement.from_table(alias!(table_name));

        let name = index.name().to_owned();
        let dtype = index.dtype().clone();
        let mut cond_or = Cond::any();

        for v in index.into_iter() {
            let expr = Expr::col(alias!(&name)).eq(try_from_value_to_svalue(v, &dtype, false)?);
            cond_or = cond_or.add(expr);
        }

        statement.cond_where(cond_or);

        Ok(statement!(self, statement))
    }
}

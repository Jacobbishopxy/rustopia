use polars::prelude::Field;
use sea_query::{Alias, Expr, Query};

use super::super::{statement, try_from_value_to_svalue, IndexOption, SaveStrategy, TableField};
use crate::{DataFrame, DdlMutation, DdlQuery, DmlMutation, DmlQuery, FabrixResult, SqlBuilder};

impl DmlMutation for SqlBuilder {
    /// given a `Dataframe`, insert it into an existing table
    fn insert(&self, table_name: &str, df: DataFrame) -> FabrixResult<String> {
        let mut statement = Query::insert();
        statement.into_table(Alias::new(table_name));
        statement.columns(vec![Alias::new(df.index.name())]);
        statement.columns(df.fields().iter().map(|c| Alias::new(c.name())));

        let column_info = df.column_info();
        for c in df.into_iter() {
            let record = c
                .data
                .into_iter()
                .zip(column_info.clone())
                .map(|(v, inf)| try_from_value_to_svalue(v, inf.0.data_type(), inf.1))
                .collect::<FabrixResult<Vec<_>>>()?;

            // make sure columns length equals records length
            statement.values(record)?;
        }

        Ok(statement!(self, statement))
    }

    /// given a `Dataframe`, in terms of indices update to an existing table
    fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &IndexOption,
    ) -> FabrixResult<Vec<String>> {
        let column_info = df.column_info();
        let indices = df.index().clone();
        let indices_type = indices.dtype().clone();
        let mut res = vec![];

        for (row, idx) in df.into_iter().zip(indices.into_iter()) {
            let mut statement = Query::update();
            statement.table(Alias::new(table_name));

            let updates = row
                .data
                .clone()
                .into_iter()
                .zip(column_info.clone())
                .map(|(v, inf)| try_from_value_to_svalue(v, inf.0.data_type(), inf.1))
                .collect::<FabrixResult<Vec<_>>>()?;
            let updates = column_info
                .clone()
                .into_iter()
                .zip(updates.into_iter())
                .map(|(inf, v)| (Alias::new(inf.0.name()), v))
                .collect::<Vec<(_, _)>>();

            statement.values(updates).and_where(
                Expr::col(Alias::new(index_option.name)).eq(try_from_value_to_svalue(
                    idx,
                    &indices_type,
                    false,
                )?),
            );

            statement!(res; self, statement)
        }

        Ok(res)
    }

    /// given a `Dataframe`, saves it with `SaveOption` strategy (transaction capability is required on executor)
    fn save(
        &self,
        table_name: &str,
        df: DataFrame,
        save_strategy: &SaveStrategy,
    ) -> FabrixResult<Vec<String>> {
        let mut res = Vec::new();
        match save_strategy {
            SaveStrategy::Replace => {
                // delete table if exists
                res.push(self.delete_table(table_name));
                // create a new table
                let index_option = IndexOption::try_from_series(df.index())?;
                res.push(self.create_table(
                    table_name,
                    &conv_fields(df.fields()),
                    Some(&index_option),
                ));
                // insert data to this new table
                res.push(self.insert(table_name, df)?)
            }
            SaveStrategy::Append => {
                // append, ignore index
                res.push(self.insert(table_name, df)?);
            }
            SaveStrategy::Upsert => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // check IDs
                res.push(self.select_exist_ids(table_name, df.index())?);
            }
            SaveStrategy::Fail => {
                // check table existence and return an integer value, 0: false, 1: true.
                res.push(self.check_table(table_name));
                // if table does not exist (the result of the previous sql execution is 0), then create a new one
                let index_option = IndexOption::try_from_series(df.index())?;
                res.push(self.create_table(
                    table_name,
                    &conv_fields(df.fields()),
                    Some(&index_option),
                ));
                // insert data to this new table
                res.push(self.insert(table_name, df)?);
            }
        }

        Ok(res)
    }
}

/// dataframe fields conversion. Temporary solution
fn conv_fields(fields: Vec<Field>) -> Vec<TableField> {
    fields.into_iter().map(|f| f.into()).collect()
}

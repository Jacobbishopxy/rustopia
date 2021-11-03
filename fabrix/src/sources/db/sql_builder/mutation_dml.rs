//! Fabrix db sql_builder dml mutation

use sea_query::{Expr, Query};

use super::{alias, statement, try_from_value_to_svalue};
use crate::{
    adt, DataFrame, DdlMutation, DdlQuery, DmlMutation, DmlQuery, FabrixResult, SqlBuilder,
};

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

    /// given a `Dataframe`, in terms of indices update to an existing table
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

    // TODO: 1. return type; 2. some return string has already been defined in executor's methods
    /// given a `Dataframe`, saves it with `SaveOption` strategy (transaction capability is required on executor)
    fn save(
        &self,
        table_name: &str,
        df: DataFrame,
        save_strategy: &adt::SaveStrategy,
    ) -> FabrixResult<Vec<String>> {
        let mut res = Vec::new();
        match save_strategy {
            adt::SaveStrategy::Replace => {
                // delete table if exists
                res.push(self.delete_table(table_name));
                // create a new table

                let mut opt = self.save(table_name, df, &adt::SaveStrategy::Fail)?;
                res.append(&mut opt);
            }
            adt::SaveStrategy::Append => {
                // append, ignore index
                res.push(self.insert(table_name, df)?);
            }
            adt::SaveStrategy::Upsert => {
                // check IDs
                res.push(self.select_exist_ids(table_name, df.index())?);

                // TODO:
                todo!()
            }
            adt::SaveStrategy::Fail => {
                // if table does not exist (the result of the previous sql execution is 0), then create a new one
                let index_option = adt::IndexOption::try_from_series(df.index())?;
                res.push(self.create_table(table_name, &df.fields(), Some(&index_option)));
                // insert data to this new table
                res.push(self.insert(table_name, df)?);
            }
        }

        Ok(res)
    }
}

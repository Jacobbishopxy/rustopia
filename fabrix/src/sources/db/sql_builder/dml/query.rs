//! Sql

use sea_query::{Alias, Expr, Query};

use super::super::{statement, try_from_value_to_svalue};
use crate::{DmlQuery, FabrixResult, Series, SqlBuilder};

impl DmlQuery for SqlBuilder {
    /// given a list of ids, check existed ids (used for `upsert` method). Make sure index contains only not-null values
    fn select_exist_ids(&self, table_name: &str, index: &Series) -> FabrixResult<String> {
        let mut statement = Query::select();
        let (index_name, index_dtype) = (index.name(), index.dtype());
        let ids = index
            .into_iter()
            .map(|i| try_from_value_to_svalue(i, index_dtype, false))
            .collect::<FabrixResult<Vec<_>>>()?;

        statement
            .column(Alias::new(index_name))
            .from(Alias::new(table_name))
            .and_where(Expr::col(Alias::new(index_name)).is_in(ids));

        Ok(statement!(self, statement))
    }
}

#[cfg(test)]
mod test_sql {

    use super::*;
    use crate::series;

    #[test]
    fn test_select_exist_ids() {
        let ids = series!("index" => [1, 2, 3, 4, 5]);
        let sql = SqlBuilder::Mysql.select_exist_ids("dev", &ids);

        println!("{:?}", sql);
    }
}

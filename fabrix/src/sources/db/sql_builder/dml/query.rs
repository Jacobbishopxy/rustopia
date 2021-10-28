//! Sql

use sea_query::*;

use super::super::{adt, statement, try_from_value_to_svalue};
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

    fn select(&self, select: &adt::Select) -> String {
        let mut statement = Query::select();

        for c in &select.columns {
            statement.column(Alias::new(&c.original_name()));
        }

        statement.from(Alias::new(&select.table));

        if let Some(flt) = &select.filter {
            filter_builder(&mut statement, flt);
        }

        if let Some(ord) = &select.order {
            ord.iter().for_each(|o| match &o.order {
                Some(ot) => match ot {
                    adt::OrderType::Asc => {
                        statement.order_by(Alias::new(&o.name), Order::Asc);
                    }
                    adt::OrderType::Desc => {
                        statement.order_by(Alias::new(&o.name), Order::Desc);
                    }
                },
                None => {
                    statement.order_by(Alias::new(&o.name), Order::Asc);
                }
            })
        }

        if let Some(l) = &select.limit {
            statement.limit(l.clone());
        }

        if let Some(o) = &select.offset {
            statement.offset(o.clone());
        }

        statement!(self, statement)
    }
}

fn filter_builder(qs: &mut SelectStatement, flt: &Vec<adt::Expression>) {
    let mut vec_cond: Vec<Cond> = vec![Cond::all()];

    flt.iter().for_each(|e| match e {
        adt::Expression::Conjunction(c) => {
            match c {
                adt::Conjunction::AND => vec_cond.push(Cond::all()),
                adt::Conjunction::OR => vec_cond.push(Cond::any()),
            };
        }
        adt::Expression::Simple(c) => {
            let tmp_expr = Expr::col(Alias::new(&c.column));
            let tmp_expr = match &c.equation {
                adt::Equation::Equal(d) => tmp_expr.eq(d),
                adt::Equation::NotEqual(d) => tmp_expr.ne(d),
                adt::Equation::Greater(d) => tmp_expr.gt(d),
                adt::Equation::GreaterEqual(d) => tmp_expr.gte(d),
                adt::Equation::Less(d) => tmp_expr.lt(d),
                adt::Equation::LessEqual(d) => tmp_expr.lte(d),
                adt::Equation::In(d) => tmp_expr.is_in(d),
                adt::Equation::Between(d) => tmp_expr.between(&d.0, &d.1),
                adt::Equation::Like(d) => tmp_expr.like(&d),
            };
            let last = vec_cond.last().unwrap().clone();
            let mut_last = vec_cond.last_mut().unwrap();
            *mut_last = last.add(tmp_expr);
        }
        adt::Expression::Nest(n) => filter_builder(qs, n),
    });

    vec_cond.iter().for_each(|c| {
        qs.cond_where(c.clone());
    });
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

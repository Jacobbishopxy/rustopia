use serde::{Deserialize, Serialize};

use crate::{DataEnum, OrderType};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Select {
    pub table: String,
    pub columns: Vec<String>,
    pub filter: Option<Vec<Expression>>,
    pub order: Option<Vec<OrderType>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectResult(pub serde_json::value::Value);

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SelectVecResult(pub Vec<SelectResult>);

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum Conjunction {
    AND,
    OR,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Equation {
    Equal(DataEnum),
    NotEqual(DataEnum),
    Greater(DataEnum),
    GreaterEqual(DataEnum),
    Less(DataEnum),
    LessEqual(DataEnum),
    In(Vec<DataEnum>),
    Between((DataEnum, DataEnum)),
    Like(String),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Condition {
    column: String,
    equation: Equation,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expression {
    Conjunction(Conjunction),
    Simple(Condition),
    Nest(Box<Vec<Expression>>),
}

#[cfg(test)]
mod tests_select {
    use super::*;

    #[test]
    fn selection() {
        let conditions = vec![
            Expression::Simple(Condition {
                column: "c3".to_owned(),
                equation: Equation::Between((DataEnum::Integer(23), DataEnum::Integer(25))),
            }),
            Expression::Conjunction(Conjunction::OR),
            Expression::Simple(Condition {
                column: "c4".to_owned(),
                equation: Equation::Between((DataEnum::Integer(1), DataEnum::Integer(8))),
            }),
            Expression::Conjunction(Conjunction::AND),
            Expression::Nest(Box::new(vec![
                Expression::Simple(Condition {
                    column: "c3".to_owned(),
                    equation: Equation::Between((DataEnum::Integer(23), DataEnum::Integer(25))),
                }),
                Expression::Conjunction(Conjunction::AND),
                Expression::Simple(Condition {
                    column: "c4".to_owned(),
                    equation: Equation::Between((DataEnum::Integer(1), DataEnum::Integer(8))),
                }),
            ])),
        ];
        let selection = Select {
            table: "sqlz".to_owned(),
            columns: vec!["c1".to_owned(), "c2".to_owned()],
            filter: Some(conditions),
            order: None,
            limit: Some(10),
            offset: Some(20),
        };

        let cvt = serde_json::to_string(&selection).unwrap();

        let res = "{\"table\":\"sqlz\",\"columns\":[\"c1\",\"c2\"],\"filter\":[{\"column\":\"c3\",\"equation\":[23,25]},\"OR\",{\"column\":\"c4\",\"equation\":[1,8]},\"AND\",[{\"column\":\"c3\",\"equation\":[23,25]},\"AND\",{\"column\":\"c4\",\"equation\":[1,8]}]],\"order\":null,\"limit\":10,\"offset\":20}";

        assert_eq!(cvt, res);
    }
}

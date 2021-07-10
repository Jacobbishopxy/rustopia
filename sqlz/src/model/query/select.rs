use serde::{Deserialize, Serialize};

use crate::{ColumnAlias, DataEnum, Order};

// TODO: Join & GroupBy

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Select {
    pub table: String,
    pub columns: Vec<ColumnAlias>,
    pub filter: Option<Vec<Expression>>,
    pub order: Option<Vec<Order>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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
    pub column: String,
    pub equation: Equation,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expression {
    Conjunction(Conjunction),
    Simple(Condition),
    Nest(Vec<Expression>),
}

#[cfg(test)]
mod tests_select {
    use super::*;

    #[test]
    fn selection() {
        let conditions = vec![
            Expression::Simple(Condition {
                column: "c1".to_owned(),
                equation: Equation::Between((DataEnum::Integer(23), DataEnum::Integer(25))),
            }),
            Expression::Conjunction(Conjunction::OR),
            Expression::Simple(Condition {
                column: "c2".to_owned(),
                equation: Equation::Equal(DataEnum::Integer(1)),
            }),
            Expression::Conjunction(Conjunction::AND),
            Expression::Nest(vec![
                Expression::Simple(Condition {
                    column: "c3".to_owned(),
                    equation: Equation::Greater(DataEnum::Integer(23)),
                }),
                Expression::Conjunction(Conjunction::AND),
                Expression::Simple(Condition {
                    column: "c4".to_owned(),
                    equation: Equation::In(vec![DataEnum::from("T1"), DataEnum::from("T2")]),
                }),
            ]),
        ];
        let selection = Select {
            table: "sqlz".to_owned(),
            columns: vec![
                ColumnAlias::Simple("c1".to_owned()),
                ColumnAlias::Alias(("c2".to_owned(), "c2_t".to_owned())),
            ],
            filter: Some(conditions),
            order: None,
            limit: Some(10),
            offset: Some(20),
        };

        let cvt = serde_json::to_string(&selection).unwrap();
        let _cvt = r##"
        {
            "table": "sqlz",
            "columns":["c1",["c2","c2_t"]],
            "filter":[
                {"column":"c1","equation":{"Between":[23,25]}},
                "OR",
                {"column":"c2","equation":{"Equal":1}},
                "AND",
                [
                    {"column":"c3","equation":{"Greater":23}},
                    "AND",
                    {"column":"c4","equation":{"In":["T1","T2"]}}
                ]
            ],
            "order":null,
            "limit":10,
            "offset":20
        }"##;

        let res = "{\"table\":\"sqlz\",\"columns\":[\"c1\",[\"c2\",\"c2_t\"]],\"filter\":[{\"column\":\"c1\",\"equation\":{\"Between\":[23,25]}},\"OR\",{\"column\":\"c2\",\"equation\":{\"Equal\":1}},\"AND\",[{\"column\":\"c3\",\"equation\":{\"Greater\":23}},\"AND\",{\"column\":\"c4\",\"equation\":{\"In\":[\"T1\",\"T2\"]}}]],\"order\":null,\"limit\":10,\"offset\":20}";

        assert_eq!(cvt, res);
    }
}

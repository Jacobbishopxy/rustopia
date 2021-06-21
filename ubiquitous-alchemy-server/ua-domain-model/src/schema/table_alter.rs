use serde::{Deserialize, Serialize};

use super::super::Column;

pub type ColumnAdd = Column;

pub type ColumnModify = Column;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnRename {
    pub from_name: String,
    pub to_name: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnDrop {
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum ColumnAlterCase {
    Add(ColumnAdd),
    Modify(ColumnModify),
    Rename(ColumnRename),
    Drop(ColumnDrop),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TableAlter {
    pub name: String,
    pub alter: Vec<ColumnAlterCase>,
}

#[cfg(test)]
mod tests_table_alter {

    use super::*;

    #[test]
    fn test_table_alter() {
        let table_alter = TableAlter {
            name: "test".to_string(),
            alter: vec![
                ColumnAlterCase::Add(ColumnAdd {
                    name: "score".to_string(),
                    col_type: crate::ColumnType::Int,
                    ..Default::default()
                }),
                ColumnAlterCase::Modify(ColumnModify {
                    name: "score".to_string(),
                    col_type: crate::ColumnType::Text,
                    ..Default::default()
                }),
            ],
        };

        println!("{:?}", serde_json::to_string(&table_alter));
    }
}

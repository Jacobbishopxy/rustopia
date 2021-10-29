//! Sql builder interface

use crate::{adt, DataFrame, FabrixResult, Series};

// DDL Query
pub trait DdlQuery {
    fn check_table(&self, table_name: &str) -> String;

    fn check_table_schema(&self, table_name: &str) -> String;

    fn list_tables(&self) -> String;

    fn get_primary_key(&self, table_name: &str) -> String;
}

// DDL Mutation
pub trait DdlMutation {
    fn create_table(
        &self,
        table_name: &str,
        columns: &Vec<adt::TableField>,
        index_option: Option<&adt::IndexOption>,
    ) -> String;

    fn delete_table(&self, table_name: &str) -> String;

    // fn alter_table(&self) -> Vec<String>;

    // fn drop_table(&self, table_name: &str) -> String;

    // fn rename_table(&self, from: &str, to: &str) -> String;

    // fn truncate_table(&self, table_name: &str) -> String;

    // fn create_index(&self) -> String;

    // fn drop_index(&self) -> String;

    // fn create_foreign_key(&self) -> String;

    // fn drop_foreign_key(&self) -> String;
}

// DML Query
pub trait DmlQuery {
    fn select_exist_ids(&self, table_name: &str, index: &Series) -> FabrixResult<String>;

    fn select(&self, select: &adt::Select) -> String;
}

// DML Mutation
pub trait DmlMutation {
    fn insert(&self, table_name: &str, df: DataFrame) -> FabrixResult<String>;

    fn update(
        &self,
        table_name: &str,
        df: DataFrame,
        index_option: &adt::IndexOption,
    ) -> FabrixResult<Vec<String>>;

    fn save(
        &self,
        table_name: &str,
        df: DataFrame,
        save_strategy: &adt::SaveStrategy,
    ) -> FabrixResult<Vec<String>>;
}

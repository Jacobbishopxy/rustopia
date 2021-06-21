use serde::{Deserialize, Serialize};

use super::super::Index;

pub type IndexCreate = Index;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IndexDrop {
    pub name: String,
    pub table: String,
}

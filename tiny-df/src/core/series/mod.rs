//! Series

use serde::{Deserialize, Serialize};

mod constructor;

use super::meta::*;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    pub data: D1,
    pub index: Vec<Index>,
}

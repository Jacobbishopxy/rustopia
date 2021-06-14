//!

use dashmap::DashMap;
// use sqlx::any::AnyPoolOptions;

pub struct DynConn {
    pub store: DashMap<String, Vec<String>>,
}

impl DynConn {
    pub fn new() -> DynConn {
        DynConn {
            store: DashMap::new(),
        }
    }

    pub fn show_keys(&self) -> Vec<String> {
        self.store.iter().map(|i| i.key().clone()).collect()
    }
}

use ffq_common::{FfqError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub uri: String,
    pub format: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn register_table(&mut self, table: TableDef) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn get(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .get(name)
            .ok_or_else(|| FfqError::Planning(format!("unknown table: {name}")))
    }

    pub fn load_from_json(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables: Vec<TableDef> =
            serde_json::from_str(&s).map_err(|e| FfqError::InvalidConfig(e.to_string()))?;
        let mut cat = Catalog::new();
        for t in tables {
            cat.register_table(t);
        }
        Ok(cat)
    }
}

use arrow_schema::{Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    #[serde(default)]
    pub uri: String,
    #[serde(default)]
    pub paths: Vec<String>,
    pub format: String,

    #[serde(default)]
    pub schema: Option<Schema>,

    #[serde(default)]
    pub stats: crate::TableStats,

    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl TableDef {
    pub fn schema_ref(&self) -> Result<SchemaRef> {
        match &self.schema {
            Some(s) => Ok(Arc::new(s.clone())),
            None => Err(FfqError::InvalidConfig(format!(
                "table '{}' has no schema; v1 analyzer needs a schema to resolve columns",
                self.name
            ))),
        }
    }

    pub fn data_paths(&self) -> Result<Vec<String>> {
        if !self.paths.is_empty() {
            return Ok(self.paths.clone());
        }
        if !self.uri.is_empty() {
            return Ok(vec![self.uri.clone()]);
        }
        Err(FfqError::InvalidConfig(format!(
            "table '{}' must define either `uri` or `paths`",
            self.name
        )))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Catalog {
    tables: HashMap<String, TableDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
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
        let tables = parse_tables_json(&s)?;
        Ok(Self::from_tables(tables))
    }

    pub fn load_from_toml(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables = parse_tables_toml(&s)?;
        Ok(Self::from_tables(tables))
    }

    pub fn load(path: &str) -> Result<Self> {
        match Path::new(path).extension().and_then(|ext| ext.to_str()) {
            Some("json") => Self::load_from_json(path),
            Some("toml") => Self::load_from_toml(path),
            Some(other) => Err(FfqError::InvalidConfig(format!(
                "unsupported catalog extension '.{other}'; use .json or .toml"
            ))),
            None => Err(FfqError::InvalidConfig(
                "catalog path must include extension .json or .toml".to_string(),
            )),
        }
    }

    fn from_tables(tables: Vec<TableDef>) -> Self {
        let mut cat = Catalog::new();
        for t in tables {
            cat.register_table(t);
        }
        cat
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CatalogFile {
    TableList(Vec<TableDef>),
    Wrapped { tables: Vec<TableDef> },
}

impl CatalogFile {
    fn into_tables(self) -> Vec<TableDef> {
        match self {
            Self::TableList(tables) => tables,
            Self::Wrapped { tables } => tables,
        }
    }
}

fn parse_tables_json(s: &str) -> Result<Vec<TableDef>> {
    let parsed: CatalogFile =
        serde_json::from_str(s).map_err(|e| FfqError::InvalidConfig(e.to_string()))?;
    Ok(parsed.into_tables())
}

fn parse_tables_toml(s: &str) -> Result<Vec<TableDef>> {
    let parsed: CatalogFile =
        toml::from_str(s).map_err(|e| FfqError::InvalidConfig(e.to_string()))?;
    Ok(parsed.into_tables())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{Catalog, TableDef};

    fn unique_path(ext: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ffq_catalog_test_{nanos}.{ext}"))
    }

    #[test]
    fn loads_catalog_from_json() {
        let path = unique_path("json");
        let payload = r#"[{"name":"t_json","uri":"./a.parquet","format":"parquet"}]"#;
        std::fs::write(&path, payload).expect("write json");

        let catalog =
            Catalog::load_from_json(path.to_str().expect("path utf8")).expect("load json");
        let table = catalog.get("t_json").expect("table exists");
        assert_eq!(
            table.data_paths().expect("paths"),
            vec!["./a.parquet".to_string()]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn loads_catalog_from_toml_wrapped_tables() {
        let path = unique_path("toml");
        let payload = r#"
            [[tables]]
            name = "t_toml"
            format = "parquet"
            paths = ["./part-1.parquet", "./part-2.parquet"]
        "#;
        std::fs::write(&path, payload).expect("write toml");

        let catalog =
            Catalog::load_from_toml(path.to_str().expect("path utf8")).expect("load toml");
        let table = catalog.get("t_toml").expect("table exists");
        assert_eq!(
            table.data_paths().expect("paths"),
            vec![
                "./part-1.parquet".to_string(),
                "./part-2.parquet".to_string()
            ]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn table_requires_uri_or_paths() {
        let t = TableDef {
            name: "t".to_string(),
            uri: String::new(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: crate::TableStats::default(),
            options: std::collections::HashMap::new(),
        };
        assert!(t.data_paths().is_err());
    }
}

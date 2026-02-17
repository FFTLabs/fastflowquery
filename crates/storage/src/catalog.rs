use arrow_schema::{Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
        Self::from_tables(tables)
    }

    pub fn load_from_toml(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables = parse_tables_toml(&s)?;
        Self::from_tables(tables)
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

    fn from_tables(tables: Vec<TableDef>) -> Result<Self> {
        let mut cat = Catalog::new();
        for t in tables {
            validate_table_schema_contract(&t)?;
            cat.register_table(t);
        }
        Ok(cat)
    }

    pub fn tables(&self) -> Vec<TableDef> {
        let mut v = self.tables.values().cloned().collect::<Vec<_>>();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn save_to_json(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = serde_json::to_string_pretty(&CatalogFile::Wrapped {
            tables: self.tables(),
        })
        .map_err(|e| FfqError::InvalidConfig(format!("catalog json encode failed: {e}")))?;
        write_atomically(path, payload.as_bytes())?;
        Ok(())
    }

    pub fn save_to_toml(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = toml::to_string_pretty(&CatalogFile::Wrapped {
            tables: self.tables(),
        })
        .map_err(|e| FfqError::InvalidConfig(format!("catalog toml encode failed: {e}")))?;
        write_atomically(path, payload.as_bytes())?;
        Ok(())
    }

    pub fn save(&self, path: &str) -> Result<()> {
        match Path::new(path).extension().and_then(|ext| ext.to_str()) {
            Some("json") => self.save_to_json(path),
            Some("toml") => self.save_to_toml(path),
            Some(other) => Err(FfqError::InvalidConfig(format!(
                "unsupported catalog extension '.{other}'; use .json or .toml"
            ))),
            None => Err(FfqError::InvalidConfig(
                "catalog path must include extension .json or .toml".to_string(),
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

fn validate_table_schema_contract(table: &TableDef) -> Result<()> {
    if table.schema.is_some() {
        return Ok(());
    }
    if format_supports_schema_inference(&table.format) {
        return Ok(());
    }
    Err(FfqError::InvalidConfig(format!(
        "table '{}' (format='{}') requires schema in catalog; only inferable formats may omit schema in v1",
        table.name, table.format
    )))
}

fn format_supports_schema_inference(format: &str) -> bool {
    matches!(format.to_ascii_lowercase().as_str(), "parquet" | "qdrant")
}

fn write_atomically(path: &str, content: &[u8]) -> Result<()> {
    let target = Path::new(path);
    let parent = target
        .parent()
        .map(std::borrow::ToOwned::to_owned)
        .unwrap_or_else(|| ".".into());
    fs::create_dir_all(&parent)?;

    let stem = target
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("catalog");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let staged = parent.join(format!(".ffq_staged_{stem}_{nanos}.tmp"));
    fs::write(&staged, content)?;

    if !target.exists() {
        fs::rename(&staged, target).map_err(|e| {
            FfqError::InvalidConfig(format!(
                "catalog commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            ))
        })?;
        return Ok(());
    }

    let backup = parent.join(format!(".ffq_backup_{stem}_{nanos}.tmp"));
    fs::rename(target, &backup).map_err(|e| {
        FfqError::InvalidConfig(format!(
            "catalog backup rename failed: {} -> {} ({e})",
            target.display(),
            backup.display()
        ))
    })?;

    match fs::rename(&staged, target) {
        Ok(_) => {
            let _ = fs::remove_file(backup);
            Ok(())
        }
        Err(e) => {
            let _ = fs::rename(&backup, target);
            let _ = fs::remove_file(&staged);
            Err(FfqError::InvalidConfig(format!(
                "catalog commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            )))
        }
    }
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

    #[test]
    fn rejects_missing_schema_for_non_inferable_format() {
        let path = unique_path("json");
        let payload = r#"[{"name":"t_csv","uri":"./a.csv","format":"csv"}]"#;
        std::fs::write(&path, payload).expect("write json");

        let err = Catalog::load_from_json(path.to_str().expect("path utf8")).expect_err("reject");
        let msg = format!("{err}");
        assert!(msg.contains("requires schema in catalog"));
        assert!(msg.contains("format='csv'"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn allows_missing_schema_for_qdrant() {
        let path = unique_path("json");
        let payload = r#"[{"name":"docs_idx","uri":"docs_idx","format":"qdrant"}]"#;
        std::fs::write(&path, payload).expect("write json");

        let catalog =
            Catalog::load_from_json(path.to_str().expect("path utf8")).expect("load qdrant");
        let table = catalog.get("docs_idx").expect("table exists");
        assert_eq!(table.format, "qdrant");
        assert!(table.schema.is_none());

        let _ = std::fs::remove_file(path);
    }
}

use arrow_schema::{Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Logical table definition persisted in catalog files (`tables.json` / `tables.toml`).
///
/// Contract:
/// - a table must provide data locations via either [`TableDef::uri`] or [`TableDef::paths`]
/// - `schema` may be omitted only for inferable formats (currently parquet and qdrant)
/// - format-specific options are carried in `options`
///
/// Persistence:
/// - this struct is serialized directly by [`Catalog::save`] and loaded by [`Catalog::load`]
/// - when schema inference/writeback is enabled at client layer, inferred schema and
///   fingerprint metadata are stored in `schema` and `options`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    /// Registry key for this table.
    pub name: String,
    /// Single path/URI location for table data.
    ///
    /// When set and [`TableDef::paths`] is empty, providers read from this location.
    #[serde(default)]
    pub uri: String,
    /// Multi-file explicit data paths.
    ///
    /// Takes precedence over [`TableDef::uri`] when non-empty.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Storage format identifier (for example `parquet`, `qdrant`).
    pub format: String,

    /// Optional table schema.
    ///
    /// For parquet, schema can be omitted and inferred by client/storage inference policies.
    #[serde(default)]
    pub schema: Option<Schema>,

    /// Optional table statistics used by optimizer heuristics.
    #[serde(default)]
    pub stats: crate::TableStats,

    /// Provider- and feature-specific options map.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl TableDef {
    /// Returns configured partition columns from table options.
    ///
    /// Contract:
    /// - options key: `partition.columns`
    /// - value format: comma-separated list (for example `ds,region`)
    #[must_use]
    pub fn partition_columns(&self) -> Vec<String> {
        self.options
            .get("partition.columns")
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    /// Returns configured partition layout convention.
    ///
    /// Supported values:
    /// - `hive` (default): path segments like `col=value/`
    #[must_use]
    pub fn partition_layout(&self) -> String {
        self.options
            .get("partition.layout")
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "hive".to_string())
    }

    /// Returns schema as [`SchemaRef`] or an error if missing.
    ///
    /// # Errors
    /// Returns an error when schema is absent.
    pub fn schema_ref(&self) -> Result<SchemaRef> {
        match &self.schema {
            Some(s) => Ok(Arc::new(s.clone())),
            None => Err(FfqError::InvalidConfig(format!(
                "table '{}' has no schema; v1 analyzer needs a schema to resolve columns",
                self.name
            ))),
        }
    }

    /// Resolves data locations in provider-consumable order.
    ///
    /// Resolution:
    /// - returns `paths` when non-empty
    /// - otherwise returns `uri` as a single entry
    ///
    /// # Errors
    /// Returns an error when neither `paths` nor `uri` is configured.
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

/// In-memory table catalog with JSON/TOML persistence helpers.
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    tables: HashMap<String, TableDef>,
}

impl Catalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Registers or replaces a table by name.
    pub fn register_table(&mut self, table: TableDef) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Retrieves a table definition by name.
    ///
    /// # Errors
    /// Returns planning error for unknown table names.
    pub fn get(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .get(name)
            .ok_or_else(|| FfqError::Planning(format!("unknown table: {name}")))
    }

    /// Loads catalog from a JSON file.
    ///
    /// # Errors
    /// Returns an error on read/parse/validation failures.
    pub fn load_from_json(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables = parse_tables_json(&s)?;
        Self::from_tables(tables)
    }

    /// Loads catalog from a TOML file.
    ///
    /// # Errors
    /// Returns an error on read/parse/validation failures.
    pub fn load_from_toml(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path)?;
        let tables = parse_tables_toml(&s)?;
        Self::from_tables(tables)
    }

    /// Loads catalog from JSON or TOML based on file extension.
    ///
    /// # Errors
    /// Returns an error for unsupported extensions or load/validation failures.
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

    /// Returns all table definitions sorted by table name.
    pub fn tables(&self) -> Vec<TableDef> {
        let mut v = self.tables.values().cloned().collect::<Vec<_>>();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Persists catalog atomically to JSON.
    ///
    /// # Errors
    /// Returns an error on encode/write/commit failures.
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

    /// Persists catalog atomically to TOML.
    ///
    /// # Errors
    /// Returns an error on encode/write/commit failures.
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

    /// Persists catalog to JSON/TOML based on file extension.
    ///
    /// # Errors
    /// Returns an error for unsupported extension or save failures.
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

/// Writes bytes to `path` using stage-then-rename atomic commit semantics.
///
/// Behavior:
/// - writes staged file under the same parent directory
/// - if target exists, renames existing target to backup, then commits staged file
/// - on commit failure, best-effort rollback restores backup
///
/// # Errors
/// Returns an error when stage/write/rename steps fail.
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

    #[test]
    fn reads_partition_options_contract() {
        let mut options = std::collections::HashMap::new();
        options.insert("partition.columns".to_string(), "ds, region".to_string());
        options.insert("partition.layout".to_string(), "hive".to_string());
        let table = TableDef {
            name: "t".to_string(),
            uri: "./x.parquet".to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: None,
            stats: crate::TableStats::default(),
            options,
        };
        assert_eq!(
            table.partition_columns(),
            vec!["ds".to_string(), "region".to_string()]
        );
        assert_eq!(table.partition_layout(), "hive");
    }
}

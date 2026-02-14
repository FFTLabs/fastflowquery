use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_common::{FfqError, Result};
use ffq_planner::{AggExpr, Expr, JoinType, LogicalPlan};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::runtime::QueryContext;
use crate::session::SharedSession;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
}

#[derive(Debug, Clone)]
pub struct DataFrame {
    session: SharedSession,
    logical_plan: LogicalPlan,
}

impl DataFrame {
    pub(crate) fn new(session: SharedSession, logical_plan: LogicalPlan) -> Self {
        Self {
            session,
            logical_plan,
        }
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// ctx.table("t") -> TableScan
    pub fn table(session: SharedSession, table: &str) -> Self {
        let plan = LogicalPlan::TableScan {
            table: table.to_string(),
            projection: None,
            filters: vec![],
        };
        Self::new(session, plan)
    }

    /// df.filter(expr)
    pub fn filter(self, predicate: Expr) -> Self {
        let plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(self.logical_plan),
        };
        Self::new(self.session, plan)
    }

    /// df.join(df2, on)
    /// on = vec![("left_key", "right_key"), ...]
    pub fn join(self, right: DataFrame, on: Vec<(String, String)>) -> Result<Self> {
        // Safety: joining DataFrames from different Engines/sessions is almost certainly a mistake.
        if !std::sync::Arc::ptr_eq(&self.session, &right.session) {
            return Err(FfqError::Planning(
                "cannot join DataFrames from different Engine instances".to_string(),
            ));
        }

        let plan = LogicalPlan::Join {
            left: Box::new(self.logical_plan),
            right: Box::new(right.logical_plan),
            on,
            join_type: JoinType::Inner,
            strategy_hint: ffq_planner::JoinStrategyHint::Auto,
        };
        Ok(Self::new(self.session, plan))
    }

    /// df.groupby(keys)
    pub fn groupby(self, keys: Vec<Expr>) -> GroupedDataFrame {
        GroupedDataFrame {
            session: self.session,
            input: self.logical_plan,
            keys,
        }
    }

    pub fn explain(&self) -> Result<String> {
        struct CatalogProvider<'a> {
            catalog: &'a ffq_storage::Catalog,
        }
        impl<'a> ffq_planner::SchemaProvider for CatalogProvider<'a> {
            fn table_schema(&self, table: &str) -> ffq_common::Result<arrow_schema::SchemaRef> {
                let t = self.catalog.get(table)?;
                t.schema_ref()
            }
        }
        impl<'a> ffq_planner::OptimizerContext for CatalogProvider<'a> {
            fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
                let t = self.catalog.get(table)?;
                Ok((t.stats.bytes, t.stats.rows))
            }
        }

        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let provider = CatalogProvider { catalog: &*cat };

        let opt = self.session.planner.optimize_only(
            self.logical_plan.clone(),
            &provider,
            &self.session.config,
        )?;

        Ok(ffq_planner::explain_logical(&opt))
    }

    /// df.collect() (async)
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let (_schema, batches) = self.execute_with_schema().await?;
        Ok(batches)
    }

    pub async fn write_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.write_parquet_with_mode(path, WriteMode::Overwrite)
            .await
    }

    pub async fn write_parquet_with_mode<P: AsRef<Path>>(
        &self,
        path: P,
        mode: WriteMode,
    ) -> Result<()> {
        let (schema, batches) = self.execute_with_schema().await?;
        let target = path.as_ref();
        if target
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        {
            if mode == WriteMode::Append {
                return Err(FfqError::Unsupported(
                    "append mode for file parquet path is unsupported; use a directory path"
                        .to_string(),
                ));
            }
            write_single_parquet_file(target, &schema, &batches)?;
            return Ok(());
        }

        let _written = write_parquet_parts(target, &schema, &batches, mode)?;
        Ok(())
    }

    pub async fn save_as_table(&self, name: &str) -> Result<()> {
        self.save_as_table_with_mode(name, WriteMode::Overwrite)
            .await
    }

    pub async fn save_as_table_with_mode(&self, name: &str, mode: WriteMode) -> Result<()> {
        if name.trim().is_empty() {
            return Err(FfqError::Planning("table name cannot be empty".to_string()));
        }
        let (schema, batches) = self.execute_with_schema().await?;
        let table_dir = PathBuf::from("./ffq_tables").join(name);
        let new_paths = write_parquet_parts(&table_dir, &schema, &batches, mode)?;

        let mut catalog = self.session.catalog.write().expect("catalog lock poisoned");
        let table = if let Ok(existing) = catalog.get(name).cloned() {
            let mut t = existing;
            t.name = name.to_string();
            t.format = "parquet".to_string();
            t.schema = Some((*schema).clone());
            match mode {
                WriteMode::Overwrite => {
                    t.uri.clear();
                    t.paths = new_paths.clone();
                }
                WriteMode::Append => {
                    t.uri.clear();
                    t.paths.extend(new_paths.clone());
                    t.paths.sort();
                    t.paths.dedup();
                }
            }
            t
        } else {
            ffq_storage::TableDef {
                name: name.to_string(),
                uri: String::new(),
                paths: new_paths,
                format: "parquet".to_string(),
                schema: Some((*schema).clone()),
                stats: ffq_storage::TableStats::default(),
                options: std::collections::HashMap::new(),
            }
        };
        catalog.register_table(table);
        Ok(())
    }

    async fn execute_with_schema(&self) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        // Ensure both SQL-built and DataFrame-built plans go through the same analyze/optimize pipeline.
        // Build a schema provider from the catalog
        struct CatalogProvider<'a> {
            catalog: &'a ffq_storage::Catalog,
        }
        impl<'a> ffq_planner::SchemaProvider for CatalogProvider<'a> {
            fn table_schema(&self, table: &str) -> ffq_common::Result<arrow_schema::SchemaRef> {
                let t = self.catalog.get(table)?;
                t.schema_ref()
            }
        }
        impl<'a> ffq_planner::OptimizerContext for CatalogProvider<'a> {
            fn table_stats(&self, table: &str) -> ffq_common::Result<(Option<u64>, Option<u64>)> {
                let t = self.catalog.get(table)?;
                Ok((t.stats.bytes, t.stats.rows))
            }
        }

        let (analyzed, catalog_snapshot) = {
            let cat_guard = self.session.catalog.read().expect("catalog lock poisoned");
            let provider = CatalogProvider {
                catalog: &*cat_guard,
            };

            let analyzed = self.session.planner.optimize_analyze(
                self.logical_plan.clone(),
                &provider,
                &self.session.config,
            )?;
            (analyzed, std::sync::Arc::new((*cat_guard).clone()))
        };

        let physical = self.session.planner.create_physical_plan(&analyzed)?;

        let ctx = QueryContext {
            batch_size_rows: self.session.config.batch_size_rows,
            mem_budget_bytes: self.session.config.mem_budget_bytes,
            spill_dir: self.session.config.spill_dir.clone(),
        };

        let stream: ffq_execution::stream::SendableRecordBatchStream = self
            .session
            .runtime
            .execute(physical, ctx, catalog_snapshot)
            .await?;
        let schema = stream.schema();

        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok((schema, batches))
    }
}

#[derive(Debug, Clone)]
pub struct GroupedDataFrame {
    session: SharedSession,
    input: LogicalPlan,
    keys: Vec<Expr>,
}

impl GroupedDataFrame {
    /// df.groupby(keys).agg(...)
    pub fn agg(self, aggs: Vec<(AggExpr, String)>) -> DataFrame {
        let plan = LogicalPlan::Aggregate {
            group_exprs: self.keys,
            aggr_exprs: aggs,
            input: Box::new(self.input),
        };
        DataFrame::new(self.session, plan)
    }
}

fn write_single_parquet_file(
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)
        .map_err(|e| FfqError::Execution(format!("parquet writer init failed: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| FfqError::Execution(format!("parquet write failed: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| FfqError::Execution(format!("parquet writer close failed: {e}")))?;
    Ok(())
}

fn write_parquet_parts(
    dir: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    mode: WriteMode,
) -> Result<Vec<String>> {
    if mode == WriteMode::Overwrite && dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;

    let start_idx = if mode == WriteMode::Append {
        next_part_index(dir)?
    } else {
        0
    };
    let file_path = dir.join(format!("part-{start_idx:05}.parquet"));
    write_single_parquet_file(&file_path, schema, batches)?;
    Ok(vec![file_path.to_string_lossy().to_string()])
}

fn next_part_index(dir: &Path) -> Result<usize> {
    let mut max_idx = None::<usize>;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let Some(name) = p.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        if !(name.starts_with("part-") && name.ends_with(".parquet")) {
            continue;
        }
        let core = &name[5..name.len() - 8];
        if let Ok(i) = core.parse::<usize>() {
            max_idx = Some(max_idx.map_or(i, |m| m.max(i)));
        }
    }
    Ok(max_idx.map_or(0, |m| m + 1))
}

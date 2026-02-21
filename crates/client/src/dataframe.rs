use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_common::{FfqError, Result};
use ffq_execution::stream::SendableRecordBatchStream;
use ffq_planner::{AggExpr, Expr, JoinType, LogicalPlan};
use ffq_storage::parquet_provider::ParquetProvider;
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::{annotate_schema_inference_metadata, read_schema_fingerprint_metadata};
use crate::runtime::{QueryContext, RuntimeStatsCollector};
use crate::session::SchemaCacheEntry;
use crate::session::SharedSession;

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

    fn table_metadata(
        &self,
        table: &str,
    ) -> ffq_common::Result<Option<ffq_planner::TableMetadata>> {
        let t = self.catalog.get(table)?;
        Ok(Some(ffq_planner::TableMetadata {
            format: t.format.clone(),
            options: t.options.clone(),
        }))
    }
}

/// Write behavior for parquet outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace destination data atomically.
    Overwrite,
    /// Append a new deterministic part file to destination directory.
    Append,
}

/// Query plan handle used for relational transformations and execution.
///
/// `DataFrame` is immutable. Each transformation returns a new plan.
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

    /// Returns the current logical plan.
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

    /// Adds a filter predicate.
    /// df.filter(expr)
    pub fn filter(self, predicate: Expr) -> Self {
        let plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(self.logical_plan),
        };
        Self::new(self.session, plan)
    }

    /// Adds an inner join between two dataframes.
    ///
    /// # Errors
    /// Returns an error when joining dataframes from different engine sessions.
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

    /// Starts grouped aggregation builder for the given grouping keys.
    /// df.groupby(keys)
    pub fn groupby(self, keys: Vec<Expr>) -> GroupedDataFrame {
        GroupedDataFrame {
            session: self.session,
            input: self.logical_plan,
            keys,
        }
    }

    /// Returns optimized logical plan text.
    ///
    /// # Errors
    /// Returns an error when schema inference, optimization, or catalog lookup fails.
    pub fn explain(&self) -> Result<String> {
        self.ensure_inferred_parquet_schemas()?;
        let cat = self.session.catalog.read().expect("catalog lock poisoned");
        let provider = CatalogProvider { catalog: &*cat };

        let opt = self.session.planner.optimize_analyze(
            self.logical_plan.clone(),
            &provider,
            &self.session.config,
        )?;
        let physical = self.session.planner.create_physical_plan(&opt)?;
        let table_stats = render_table_stats_section(&opt, &*cat);
        Ok(format!(
            "== Logical Plan ==\n{}\n== Physical Plan ==\n{}\n== Table Stats ==\n{}",
            ffq_planner::explain_logical(&opt),
            ffq_planner::explain_physical(&physical),
            table_stats
        ))
    }

    /// Executes this query and returns explain text with runtime stage/operator statistics.
    ///
    /// # Errors
    /// Returns an error when planning or execution fails.
    pub async fn explain_analyze(&self) -> Result<String> {
        let _ = self.collect().await?;
        let explain = self.explain()?;
        let stats = self
            .session
            .last_query_stats_report
            .read()
            .expect("query stats lock poisoned")
            .clone()
            .unwrap_or_else(|| "no runtime stats captured".to_string());
        Ok(format!("{explain}\n== Runtime Stats ==\n{stats}"))
    }

    /// df.collect() (async)
    ///
    /// # Examples
    /// ```no_run
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// let df = engine.sql("SELECT 1 as one")?;
    /// let batches = futures::executor::block_on(df.collect())?;
    /// assert!(!batches.is_empty());
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error when planning or execution fails.
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let stream = self.collect_stream().await?;
        stream.try_collect().await
    }

    /// Executes this plan and returns a streaming batch result.
    ///
    /// # Errors
    /// Returns an error when planning or execution fails.
    pub async fn collect_stream(&self) -> Result<SendableRecordBatchStream> {
        self.create_execution_stream().await
    }

    /// Executes this plan and writes output to parquet, replacing destination by default.
    ///
    /// If `path` ends with `.parquet`, output is written to that file.
    /// Otherwise, `path` is treated as output directory.
    ///
    /// # Examples
    /// ```no_run
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// let df = engine.sql("SELECT 1 as id")?;
    /// futures::executor::block_on(df.write_parquet("/tmp/ffq_out"))?;
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error when planning, execution, or file commit fails.
    pub async fn write_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.write_parquet_with_mode(path, WriteMode::Overwrite)
            .await
    }

    /// Executes this plan and writes output to parquet with explicit write mode.
    ///
    /// Append is only supported for directory paths.
    ///
    /// # Errors
    /// Returns an error for unsupported file append mode or write/commit failures.
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
            write_single_parquet_file_durable(target, &schema, &batches)?;
            return Ok(());
        }

        let _written = write_parquet_parts_durable(target, &schema, &batches, mode)?;
        Ok(())
    }

    /// Executes this plan and saves output as a managed table with overwrite mode.
    ///
    /// # Errors
    /// Returns an error when execution, persistence, or catalog update fails.
    pub async fn save_as_table(&self, name: &str) -> Result<()> {
        self.save_as_table_with_mode(name, WriteMode::Overwrite)
            .await
    }

    /// Executes this plan and saves output as a managed table with explicit write mode.
    ///
    /// Overwrite replaces table data paths; append adds new parts and deduplicates paths.
    ///
    /// # Examples
    /// ```no_run
    /// use ffq_client::Engine;
    /// use ffq_common::EngineConfig;
    ///
    /// let engine = Engine::new(EngineConfig::default())?;
    /// let df = engine.sql("SELECT 42 as answer")?;
    /// futures::executor::block_on(df.save_as_table("answers"))?;
    /// let rows = futures::executor::block_on(engine.sql("SELECT answer FROM answers")?.collect())?;
    /// assert!(!rows.is_empty());
    /// # Ok::<(), ffq_common::FfqError>(())
    /// ```
    ///
    /// # Errors
    /// Returns an error when name validation, execution, write, or catalog persistence fails.
    pub async fn save_as_table_with_mode(&self, name: &str, mode: WriteMode) -> Result<()> {
        if name.trim().is_empty() {
            return Err(FfqError::Planning("table name cannot be empty".to_string()));
        }
        let (schema, batches) = self.execute_with_schema().await?;
        let table_dir = self.session.managed_table_path(name);
        let new_paths = write_parquet_parts_durable(&table_dir, &schema, &batches, mode)?;

        {
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
        }
        self.session.persist_catalog()?;
        Ok(())
    }

    async fn execute_with_schema(&self) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let stream = self.create_execution_stream().await?;
        let schema = stream.schema();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok((schema, batches))
    }

    async fn create_execution_stream(&self) -> Result<SendableRecordBatchStream> {
        self.ensure_inferred_parquet_schemas()?;
        // Ensure both SQL-built and DataFrame-built plans go through the same analyze/optimize pipeline.
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

        let stats_collector = Arc::new(RuntimeStatsCollector::default());
        let ctx = QueryContext {
            batch_size_rows: self.session.config.batch_size_rows,
            mem_budget_bytes: self.session.config.mem_budget_bytes,
            spill_trigger_ratio_num: 1,
            spill_trigger_ratio_den: 1,
            broadcast_threshold_bytes: self.session.config.broadcast_threshold_bytes,
            join_radix_bits: self.session.config.join_radix_bits,
            join_bloom_enabled: self.session.config.join_bloom_enabled,
            join_bloom_bits: self.session.config.join_bloom_bits,
            spill_dir: self.session.config.spill_dir.clone(),
            stats_collector: Some(Arc::clone(&stats_collector)),
        };

        let stream = self
            .session
            .runtime
            .execute(
                physical,
                ctx,
                catalog_snapshot,
                Arc::clone(&self.session.physical_registry),
            )
            .await?;
        let report = stats_collector.render_report();
        {
            let mut slot = self
                .session
                .last_query_stats_report
                .write()
                .expect("query stats lock poisoned");
            *slot = report;
        }
        Ok(stream)
    }

    fn ensure_inferred_parquet_schemas(&self) -> Result<()> {
        let mut names = Vec::new();
        collect_table_refs(&self.logical_plan, &mut names);
        let mut seen = HashSet::new();
        names.retain(|n| seen.insert(n.clone()));

        if names.is_empty() {
            return Ok(());
        }

        let mut catalog_changed = false;
        {
            let mut catalog = self.session.catalog.write().expect("catalog lock poisoned");
            for name in names {
                let Some(mut table) = catalog.get(&name).ok().cloned() else {
                    continue;
                };
                if !table.format.eq_ignore_ascii_case("parquet") {
                    continue;
                }
                if !self.session.config.schema_inference.allows_inference()
                    && table.schema.is_none()
                {
                    continue;
                }
                let paths = table.data_paths()?;
                let fingerprint = ParquetProvider::fingerprint_paths(&paths)?;
                let mut cache = self
                    .session
                    .schema_cache
                    .write()
                    .expect("schema cache lock poisoned");

                let mut refreshed = false;
                let schema = if let Some(entry) = cache.get(&name) {
                    if entry.fingerprint == fingerprint {
                        entry.schema.clone()
                    } else if matches!(
                        self.session.config.schema_drift_policy,
                        ffq_common::SchemaDriftPolicy::Fail
                    ) {
                        return Err(FfqError::InvalidConfig(format!(
                            "schema drift detected for table '{}'; file fingerprint changed",
                            name
                        )));
                    } else {
                        refreshed = true;
                        ParquetProvider::infer_parquet_schema_with_policy(
                            &paths,
                            self.session.config.schema_inference.is_permissive_merge(),
                        )
                        .map_err(|e| {
                            FfqError::InvalidConfig(format!(
                                "schema inference failed for table '{}': {e}",
                                name
                            ))
                        })?
                    }
                } else if let Some(existing) = &table.schema {
                    let stored_fingerprint = read_schema_fingerprint_metadata(&table)?;
                    if let Some(stored) = stored_fingerprint {
                        if stored != fingerprint {
                            if matches!(
                                self.session.config.schema_drift_policy,
                                ffq_common::SchemaDriftPolicy::Fail
                            ) {
                                return Err(FfqError::InvalidConfig(format!(
                                    "schema drift detected for table '{}'; file fingerprint changed",
                                    name
                                )));
                            }
                            refreshed = true;
                            ParquetProvider::infer_parquet_schema_with_policy(
                                &paths,
                                self.session.config.schema_inference.is_permissive_merge(),
                            )
                            .map_err(|e| {
                                FfqError::InvalidConfig(format!(
                                    "schema inference failed for table '{}': {e}",
                                    name
                                ))
                            })?
                        } else {
                            existing.clone()
                        }
                    } else {
                        existing.clone()
                    }
                } else {
                    refreshed = true;
                    ParquetProvider::infer_parquet_schema_with_policy(
                        &paths,
                        self.session.config.schema_inference.is_permissive_merge(),
                    )
                    .map_err(|e| {
                        FfqError::InvalidConfig(format!(
                            "schema inference failed for table '{}': {e}",
                            name
                        ))
                    })?
                };

                cache.insert(
                    name.clone(),
                    SchemaCacheEntry {
                        schema: schema.clone(),
                        fingerprint: fingerprint.clone(),
                    },
                );
                if table.schema.as_ref() != Some(&schema) {
                    table.schema = Some(schema);
                    refreshed = true;
                }
                if refreshed {
                    annotate_schema_inference_metadata(&mut table, &fingerprint)?;
                    catalog_changed = true;
                }
                catalog.register_table(table);
            }
        }

        if catalog_changed && self.session.config.schema_writeback {
            self.session.persist_catalog()?;
        }
        Ok(())
    }
}

/// Builder for grouped aggregations produced by [`DataFrame::groupby`].
#[derive(Debug, Clone)]
pub struct GroupedDataFrame {
    session: SharedSession,
    input: LogicalPlan,
    keys: Vec<Expr>,
}

impl GroupedDataFrame {
    /// Adds aggregate expressions and returns the resulting query dataframe.
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

fn collect_table_refs(plan: &LogicalPlan, out: &mut Vec<String>) {
    match plan {
        LogicalPlan::TableScan { table, .. } => out.push(table.clone()),
        LogicalPlan::Projection { input, .. } => collect_table_refs(input, out),
        LogicalPlan::Filter { input, .. } => collect_table_refs(input, out),
        LogicalPlan::InSubqueryFilter {
            input, subquery, ..
        } => {
            collect_table_refs(input, out);
            collect_table_refs(subquery, out);
        }
        LogicalPlan::ExistsSubqueryFilter {
            input, subquery, ..
        } => {
            collect_table_refs(input, out);
            collect_table_refs(subquery, out);
        }
        LogicalPlan::ScalarSubqueryFilter {
            input, subquery, ..
        } => {
            collect_table_refs(input, out);
            collect_table_refs(subquery, out);
        }
        LogicalPlan::Join { left, right, .. } => {
            collect_table_refs(left, out);
            collect_table_refs(right, out);
        }
        LogicalPlan::Aggregate { input, .. } => collect_table_refs(input, out),
        LogicalPlan::Window { input, .. } => collect_table_refs(input, out),
        LogicalPlan::Limit { input, .. } => collect_table_refs(input, out),
        LogicalPlan::TopKByScore { input, .. } => collect_table_refs(input, out),
        LogicalPlan::UnionAll { left, right } => {
            collect_table_refs(left, out);
            collect_table_refs(right, out);
        }
        LogicalPlan::CteRef { plan, .. } => collect_table_refs(plan, out),
        LogicalPlan::VectorTopK { table, .. } => out.push(table.clone()),
        LogicalPlan::InsertInto { input, .. } => {
            // Insert target is a write sink; schema inference/fingerprint checks are only
            // needed for read-side tables referenced by the input query.
            collect_table_refs(input, out);
        }
    }
}

fn render_table_stats_section(plan: &LogicalPlan, catalog: &ffq_storage::Catalog) -> String {
    let mut names = Vec::new();
    collect_table_refs(plan, &mut names);
    let mut seen = std::collections::HashSet::new();
    names.retain(|n| seen.insert(n.clone()));
    if names.is_empty() {
        return "no table scans".to_string();
    }
    let mut lines = Vec::new();
    for name in names {
        match catalog.get(&name) {
            Ok(table) => {
                let bytes = table
                    .stats
                    .bytes
                    .map(|b| b.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let rows = table
                    .stats
                    .rows
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let file_count = table
                    .options
                    .get("stats.parquet.file_count")
                    .cloned()
                    .unwrap_or_else(|| "n/a".to_string());
                lines.push(format!(
                    "- {name}: bytes={bytes} rows={rows} file_count={file_count}"
                ));
            }
            Err(_) => lines.push(format!("- {name}: missing from catalog")),
        }
    }
    lines.join("\n")
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

fn write_single_parquet_file_durable(
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    let stage = temp_sibling_path(path, "staged");
    write_single_parquet_file(&stage, schema, batches)?;
    if let Err(err) = replace_file_atomically(&stage, path) {
        let _ = fs::remove_file(&stage);
        return Err(err);
    }
    Ok(())
}

fn write_parquet_parts_durable(
    dir: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    mode: WriteMode,
) -> Result<Vec<String>> {
    match mode {
        WriteMode::Overwrite => {
            let stage_dir = temp_sibling_path(dir, "staged");
            fs::create_dir_all(&stage_dir)?;
            let stage_file = stage_dir.join("part-00000.parquet");
            if let Err(err) = write_single_parquet_file(&stage_file, schema, batches) {
                let _ = fs::remove_dir_all(&stage_dir);
                return Err(err);
            }
            if let Err(err) = replace_dir_atomically(&stage_dir, dir) {
                let _ = fs::remove_dir_all(&stage_dir);
                return Err(err);
            }
            let file_path = dir.join("part-00000.parquet");
            Ok(vec![file_path.to_string_lossy().to_string()])
        }
        WriteMode::Append => {
            fs::create_dir_all(dir)?;
            let start_idx = next_part_index(dir)?;
            let final_file = dir.join(format!("part-{start_idx:05}.parquet"));
            let stage_file = temp_sibling_path(&final_file, "staged");
            if let Err(err) = write_single_parquet_file(&stage_file, schema, batches) {
                let _ = fs::remove_file(&stage_file);
                return Err(err);
            }
            if let Err(err) = fs::rename(&stage_file, &final_file) {
                let _ = fs::remove_file(&stage_file);
                return Err(FfqError::Execution(format!(
                    "append commit failed for {}: {err}",
                    final_file.display()
                )));
            }
            Ok(vec![final_file.to_string_lossy().to_string()])
        }
    }
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

fn temp_sibling_path(path: &Path, label: &str) -> PathBuf {
    let parent = path
        .parent()
        .map(std::borrow::ToOwned::to_owned)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("target");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    parent.join(format!(".ffq_{label}_{stem}_{nanos}.tmp"))
}

fn replace_file_atomically(staged: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    if !target.exists() {
        fs::rename(staged, target).map_err(|e| {
            FfqError::Execution(format!(
                "file commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            ))
        })?;
        return Ok(());
    }

    let backup = temp_sibling_path(target, "backup");
    fs::rename(target, &backup).map_err(|e| {
        FfqError::Execution(format!(
            "file backup rename failed: {} -> {} ({e})",
            target.display(),
            backup.display()
        ))
    })?;

    match fs::rename(staged, target) {
        Ok(_) => {
            let _ = fs::remove_file(backup);
            Ok(())
        }
        Err(e) => {
            let _ = fs::rename(&backup, target);
            Err(FfqError::Execution(format!(
                "file commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            )))
        }
    }
}

fn replace_dir_atomically(staged: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    if !target.exists() {
        fs::rename(staged, target).map_err(|e| {
            FfqError::Execution(format!(
                "dir commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            ))
        })?;
        return Ok(());
    }

    let backup = temp_sibling_path(target, "backup");
    fs::rename(target, &backup).map_err(|e| {
        FfqError::Execution(format!(
            "dir backup rename failed: {} -> {} ({e})",
            target.display(),
            backup.display()
        ))
    })?;

    match fs::rename(staged, target) {
        Ok(_) => {
            let _ = fs::remove_dir_all(backup);
            Ok(())
        }
        Err(e) => {
            let _ = fs::rename(&backup, target);
            Err(FfqError::Execution(format!(
                "dir commit failed: {} -> {} ({e})",
                staged.display(),
                target.display()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::CatalogProvider;
    use ffq_planner::OptimizerContext;

    #[test]
    fn optimizer_context_exposes_table_metadata_from_catalog() {
        let mut catalog = ffq_storage::Catalog::new();
        catalog.register_table(ffq_storage::TableDef {
            name: "docs_idx".to_string(),
            uri: String::new(),
            paths: Vec::new(),
            format: "qdrant".to_string(),
            schema: None,
            stats: ffq_storage::TableStats::default(),
            options: HashMap::from([
                ("qdrant.collection".to_string(), "docs".to_string()),
                (
                    "qdrant.endpoint".to_string(),
                    "http://127.0.0.1:6334".to_string(),
                ),
            ]),
        });
        let ctx = CatalogProvider { catalog: &catalog };

        let fmt = ctx
            .table_format("docs_idx")
            .expect("table format")
            .expect("format");
        assert_eq!(fmt, "qdrant");

        let opts = ctx
            .table_options("docs_idx")
            .expect("table options")
            .expect("options");
        assert_eq!(
            opts.get("qdrant.collection").expect("collection option"),
            "docs"
        );
    }
}

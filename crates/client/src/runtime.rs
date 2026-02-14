use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Array, Int64Builder, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ffq_common::{FfqError, Result};
use ffq_execution::{compile_expr, SendableRecordBatchStream, StreamAdapter, TaskContext};
use ffq_planner::{AggExpr, ExchangeExec, Expr, PhysicalPlan};
use ffq_storage::parquet_provider::ParquetProvider;
use ffq_storage::{Catalog, StorageProvider};
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct QueryContext {
    pub batch_size_rows: usize,
    pub mem_budget_bytes: usize,
    pub spill_dir: String,
}

/// Runtime = something that can execute a PhysicalPlan and return a stream of RecordBatches.
pub trait Runtime: Send + Sync + Debug {
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>>;

    fn shutdown(&self) -> BoxFuture<'static, Result<()>> {
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug, Default)]
pub struct EmbeddedRuntime;

impl EmbeddedRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for EmbeddedRuntime {
    fn execute(
        &self,
        plan: PhysicalPlan,
        ctx: QueryContext,
        catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            let exec = execute_plan(plan, ctx, catalog).await?;
            let stream = futures::stream::iter(exec.batches.into_iter().map(Ok));
            Ok(Box::pin(StreamAdapter::new(exec.schema, stream)) as SendableRecordBatchStream)
        }
        .boxed()
    }
}

struct ExecOutput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

fn execute_plan(
    plan: PhysicalPlan,
    ctx: QueryContext,
    catalog: Arc<Catalog>,
) -> BoxFuture<'static, Result<ExecOutput>> {
    async move {
        match plan {
            PhysicalPlan::ParquetScan(scan) => {
                let table = catalog.get(&scan.table)?.clone();
                let provider = ParquetProvider::new();
                let node = provider.scan(
                    &table,
                    scan.projection,
                    scan.filters.into_iter().map(|f| format!("{f:?}")).collect(),
                )?;
                let stream = node.execute(Arc::new(TaskContext {
                    batch_size_rows: ctx.batch_size_rows,
                    mem_budget_bytes: ctx.mem_budget_bytes,
                }))?;
                let schema = stream.schema();
                let batches = stream.try_collect::<Vec<RecordBatch>>().await?;
                Ok(ExecOutput { schema, batches })
            }
            PhysicalPlan::Exchange(exchange) => match exchange {
                ExchangeExec::ShuffleWrite(x) => execute_plan(*x.input, ctx, catalog).await,
                ExchangeExec::ShuffleRead(x) => execute_plan(*x.input, ctx, catalog).await,
                ExchangeExec::Broadcast(x) => execute_plan(*x.input, ctx, catalog).await,
            },
            PhysicalPlan::PartialHashAggregate(agg) => {
                let child = execute_plan(*agg.input, ctx.clone(), catalog).await?;
                run_hash_aggregate(
                    child,
                    agg.group_exprs,
                    agg.aggr_exprs,
                    AggregateMode::Partial,
                    &ctx,
                )
            }
            PhysicalPlan::FinalHashAggregate(agg) => {
                let child = execute_plan(*agg.input, ctx.clone(), catalog).await?;
                run_hash_aggregate(
                    child,
                    agg.group_exprs,
                    agg.aggr_exprs,
                    AggregateMode::Final,
                    &ctx,
                )
            }
            other => Err(FfqError::Unsupported(format!(
                "embedded runtime does not support operator yet: {other:?}"
            ))),
        }
    }
    .boxed()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateMode {
    Partial,
    Final,
}

#[derive(Debug, Clone)]
struct AggSpec {
    expr: AggExpr,
    name: String,
    out_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ScalarValue {
    Int64(i64),
    Float64Bits(u64),
    Utf8(String),
    Boolean(bool),
    Null,
}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Int64(v) => {
                0_u8.hash(state);
                v.hash(state);
            }
            Self::Float64Bits(v) => {
                1_u8.hash(state);
                v.hash(state);
            }
            Self::Utf8(v) => {
                2_u8.hash(state);
                v.hash(state);
            }
            Self::Boolean(v) => {
                3_u8.hash(state);
                v.hash(state);
            }
            Self::Null => 4_u8.hash(state),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggState {
    Count(i64),
    SumInt(i64),
    SumFloat(f64),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpillRow {
    key: Vec<ScalarValue>,
    states: Vec<AggState>,
}

fn run_hash_aggregate(
    child: ExecOutput,
    group_exprs: Vec<Expr>,
    aggr_exprs: Vec<(AggExpr, String)>,
    mode: AggregateMode,
    ctx: &QueryContext,
) -> Result<ExecOutput> {
    let input_schema = child.schema;
    let specs = build_agg_specs(&aggr_exprs, &input_schema, &group_exprs, mode)?;

    let mut groups: HashMap<Vec<ScalarValue>, Vec<AggState>> = HashMap::new();
    let mut spills = Vec::<PathBuf>::new();

    for batch in &child.batches {
        accumulate_batch(
            mode,
            &specs,
            &group_exprs,
            &input_schema,
            batch,
            &mut groups,
        )?;
        maybe_spill(&mut groups, &mut spills, ctx)?;
    }

    if group_exprs.is_empty() && groups.is_empty() {
        groups.insert(vec![], init_states(&specs));
    }

    if !groups.is_empty() {
        maybe_spill(&mut groups, &mut spills, ctx)?;
    }

    if !spills.is_empty() {
        for path in &spills {
            merge_spill_file(path, &mut groups)?;
            let _ = fs::remove_file(path);
        }
    }

    build_output(groups, &specs, &group_exprs, &input_schema, mode)
}

fn build_agg_specs(
    aggr_exprs: &[(AggExpr, String)],
    input_schema: &SchemaRef,
    group_exprs: &[Expr],
    mode: AggregateMode,
) -> Result<Vec<AggSpec>> {
    let mut specs = Vec::with_capacity(aggr_exprs.len());
    for (idx, (expr, name)) in aggr_exprs.iter().enumerate() {
        let out_type = match mode {
            AggregateMode::Partial => match expr {
                AggExpr::Count(_) => DataType::Int64,
                AggExpr::Sum(e) | AggExpr::Min(e) | AggExpr::Max(e) => {
                    expr_data_type(e, input_schema)?
                }
                AggExpr::Avg(_) => DataType::Float64,
            },
            AggregateMode::Final => {
                let col_idx = group_exprs.len() + idx;
                input_schema.field(col_idx).data_type().clone()
            }
        };
        specs.push(AggSpec {
            expr: expr.clone(),
            name: name.clone(),
            out_type,
        });
    }
    Ok(specs)
}

fn expr_data_type(expr: &Expr, schema: &SchemaRef) -> Result<DataType> {
    let compiled = compile_expr(expr, schema)?;
    Ok(compiled.data_type())
}

fn init_states(specs: &[AggSpec]) -> Vec<AggState> {
    specs
        .iter()
        .map(|s| match s.expr {
            AggExpr::Count(_) => AggState::Count(0),
            AggExpr::Sum(_) => match s.out_type {
                DataType::Int64 => AggState::SumInt(0),
                _ => AggState::SumFloat(0.0),
            },
            AggExpr::Min(_) => AggState::Min(None),
            AggExpr::Max(_) => AggState::Max(None),
            AggExpr::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
        })
        .collect()
}

fn accumulate_batch(
    mode: AggregateMode,
    specs: &[AggSpec],
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    batch: &RecordBatch,
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
) -> Result<()> {
    let group_arrays = match mode {
        AggregateMode::Partial => {
            let mut arrays = Vec::with_capacity(group_exprs.len());
            for expr in group_exprs {
                let compiled = compile_expr(expr, input_schema)?;
                arrays.push(compiled.evaluate(batch)?);
            }
            arrays
        }
        AggregateMode::Final => {
            let mut arrays = Vec::with_capacity(group_exprs.len());
            for idx in 0..group_exprs.len() {
                arrays.push(batch.column(idx).clone());
            }
            arrays
        }
    };

    let agg_arrays = match mode {
        AggregateMode::Partial => {
            let mut arrays = Vec::with_capacity(specs.len());
            for spec in specs {
                let expr = match &spec.expr {
                    AggExpr::Count(e)
                    | AggExpr::Sum(e)
                    | AggExpr::Min(e)
                    | AggExpr::Max(e)
                    | AggExpr::Avg(e) => e,
                };
                let compiled = compile_expr(expr, input_schema)?;
                arrays.push(compiled.evaluate(batch)?);
            }
            arrays
        }
        AggregateMode::Final => {
            let mut arrays = Vec::with_capacity(specs.len());
            for idx in 0..specs.len() {
                arrays.push(batch.column(group_exprs.len() + idx).clone());
            }
            arrays
        }
    };

    let avg_count_arrays = if mode == AggregateMode::Final {
        let mut map = HashMap::<String, ArrayRef>::new();
        for spec in specs {
            if matches!(spec.expr, AggExpr::Avg(_)) {
                let key = avg_count_col_name(&spec.name);
                if let Ok(i) = input_schema.index_of(&key) {
                    map.insert(spec.name.clone(), batch.column(i).clone());
                }
            }
        }
        map
    } else {
        HashMap::new()
    };

    for row in 0..batch.num_rows() {
        let key = group_arrays
            .iter()
            .map(|a| scalar_from_array(a, row))
            .collect::<Result<Vec<_>>>()?;

        let state_vec = groups.entry(key).or_insert_with(|| init_states(specs));

        for (idx, spec) in specs.iter().enumerate() {
            let value = scalar_from_array(&agg_arrays[idx], row)?;
            update_state(
                &mut state_vec[idx],
                spec,
                value,
                mode,
                avg_count_arrays.get(&spec.name).map(|a| (a, row)),
            )?;
        }
    }

    Ok(())
}

fn update_state(
    state: &mut AggState,
    spec: &AggSpec,
    value: ScalarValue,
    mode: AggregateMode,
    avg_count_src: Option<(&ArrayRef, usize)>,
) -> Result<()> {
    match state {
        AggState::Count(acc) => {
            if mode == AggregateMode::Final {
                if let ScalarValue::Int64(v) = value {
                    *acc += v;
                }
            } else if value != ScalarValue::Null {
                *acc += 1;
            }
        }
        AggState::SumInt(acc) => {
            if let ScalarValue::Int64(v) = value {
                *acc += v;
            }
        }
        AggState::SumFloat(acc) => {
            if let Some(v) = as_f64(&value) {
                *acc += v;
            }
        }
        AggState::Min(cur) => {
            if value != ScalarValue::Null {
                match cur {
                    None => *cur = Some(value),
                    Some(existing) => {
                        if scalar_lt(&value, existing)? {
                            *cur = Some(value);
                        }
                    }
                }
            }
        }
        AggState::Max(cur) => {
            if value != ScalarValue::Null {
                match cur {
                    None => *cur = Some(value),
                    Some(existing) => {
                        if scalar_gt(&value, existing)? {
                            *cur = Some(value);
                        }
                    }
                }
            }
        }
        AggState::Avg { sum, count } => match mode {
            AggregateMode::Partial => {
                if let Some(v) = as_f64(&value) {
                    *sum += v;
                    *count += 1;
                }
            }
            AggregateMode::Final => {
                if let Some(v) = as_f64(&value) {
                    *sum += v;
                }
                let add_count = if let Some((arr, row)) = avg_count_src {
                    match scalar_from_array(arr, row)? {
                        ScalarValue::Int64(v) => v,
                        _ => 0,
                    }
                } else if value != ScalarValue::Null {
                    1
                } else {
                    0
                };
                *count += add_count;
            }
        },
    }

    if let (AggExpr::Count(_), AggState::Count(acc)) = (&spec.expr, state) {
        if *acc < 0 {
            return Err(FfqError::Execution("count overflow".to_string()));
        }
    }

    Ok(())
}

fn build_output(
    groups: HashMap<Vec<ScalarValue>, Vec<AggState>>,
    specs: &[AggSpec],
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    mode: AggregateMode,
) -> Result<ExecOutput> {
    let mut keys: Vec<Vec<ScalarValue>> = groups.keys().cloned().collect();
    keys.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

    let mut fields = Vec::<Field>::new();
    let mut cols = Vec::<Vec<ScalarValue>>::new();

    for gidx in 0..group_exprs.len() {
        let (name, dt) = group_field(group_exprs, input_schema, gidx)?;
        fields.push(Field::new(&name, dt.clone(), true));
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            values.push(key[gidx].clone());
        }
        cols.push(values);
    }

    let mut avg_hidden_counts: Vec<(String, Vec<ScalarValue>)> = Vec::new();

    for (aidx, spec) in specs.iter().enumerate() {
        fields.push(Field::new(&spec.name, spec.out_type.clone(), true));
        let mut values = Vec::with_capacity(keys.len());
        let mut hidden_counts = Vec::new();

        for key in &keys {
            let states = groups
                .get(key)
                .ok_or_else(|| FfqError::Execution("missing aggregate state".to_string()))?;
            let state = &states[aidx];
            values.push(state_to_scalar(state, &spec.expr, mode));
            if matches!(spec.expr, AggExpr::Avg(_)) {
                let c = match state {
                    AggState::Avg { count, .. } => *count,
                    _ => 0,
                };
                hidden_counts.push(ScalarValue::Int64(c));
            }
        }

        cols.push(values);

        if mode == AggregateMode::Partial && matches!(spec.expr, AggExpr::Avg(_)) {
            avg_hidden_counts.push((avg_count_col_name(&spec.name), hidden_counts));
        }
    }

    for (name, values) in avg_hidden_counts {
        fields.push(Field::new(&name, DataType::Int64, true));
        cols.push(values);
    }

    let schema = Arc::new(Schema::new(fields));
    let arrays = cols
        .iter()
        .enumerate()
        .map(|(idx, col)| scalars_to_array(col, schema.field(idx).data_type()))
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| FfqError::Execution(format!("aggregate output batch failed: {e}")))?;

    Ok(ExecOutput {
        schema,
        batches: vec![batch],
    })
}

fn group_field(
    group_exprs: &[Expr],
    input_schema: &SchemaRef,
    idx: usize,
) -> Result<(String, DataType)> {
    match &group_exprs[idx] {
        Expr::ColumnRef { name, index } => Ok((
            name.clone(),
            if *index < input_schema.fields().len() {
                input_schema.field(*index).data_type().clone()
            } else {
                input_schema.field(idx).data_type().clone()
            },
        )),
        Expr::Column(name) => {
            let i = input_schema
                .index_of(name)
                .map_err(|e| FfqError::Execution(format!("unknown group column: {e}")))?;
            Ok((name.clone(), input_schema.field(i).data_type().clone()))
        }
        e => Ok((format!("{e:?}"), DataType::Utf8)),
    }
}

fn state_to_scalar(state: &AggState, expr: &AggExpr, mode: AggregateMode) -> ScalarValue {
    match (state, expr) {
        (AggState::Count(v), _) => ScalarValue::Int64(*v),
        (AggState::SumInt(v), _) => ScalarValue::Int64(*v),
        (AggState::SumFloat(v), _) => ScalarValue::Float64Bits(v.to_bits()),
        (AggState::Min(Some(v)), _) => v.clone(),
        (AggState::Min(None), _) => ScalarValue::Null,
        (AggState::Max(Some(v)), _) => v.clone(),
        (AggState::Max(None), _) => ScalarValue::Null,
        (AggState::Avg { sum, count }, AggExpr::Avg(_)) => {
            if mode == AggregateMode::Partial {
                ScalarValue::Float64Bits(sum.to_bits())
            } else if *count == 0 {
                ScalarValue::Null
            } else {
                ScalarValue::Float64Bits((sum / (*count as f64)).to_bits())
            }
        }
        _ => ScalarValue::Null,
    }
}

fn maybe_spill(
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
    spills: &mut Vec<PathBuf>,
    ctx: &QueryContext,
) -> Result<()> {
    if groups.is_empty() || ctx.mem_budget_bytes == 0 {
        return Ok(());
    }

    let estimated = estimate_groups_bytes(groups);
    if estimated <= ctx.mem_budget_bytes {
        return Ok(());
    }

    fs::create_dir_all(&ctx.spill_dir)?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let path = PathBuf::from(&ctx.spill_dir).join(format!("agg_spill_{suffix}.jsonl"));

    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    for (key, states) in groups.iter() {
        let row = SpillRow {
            key: key.clone(),
            states: states.clone(),
        };
        let line = serde_json::to_string(&row)
            .map_err(|e| FfqError::Execution(format!("spill serialize failed: {e}")))?;
        writer.write_all(line.as_bytes()).map_err(FfqError::from)?;
        writer.write_all(b"\n").map_err(FfqError::from)?;
    }
    writer.flush().map_err(FfqError::from)?;

    groups.clear();
    spills.push(path);
    Ok(())
}

fn merge_spill_file(
    path: &PathBuf,
    groups: &mut HashMap<Vec<ScalarValue>, Vec<AggState>>,
) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SpillRow = serde_json::from_str(&line)
            .map_err(|e| FfqError::Execution(format!("spill deserialize failed: {e}")))?;
        if let Some(existing) = groups.get_mut(&row.key) {
            merge_states(existing, &row.states)?;
        } else {
            groups.insert(row.key, row.states);
        }
    }
    Ok(())
}

fn merge_states(target: &mut [AggState], other: &[AggState]) -> Result<()> {
    if target.len() != other.len() {
        return Err(FfqError::Execution(
            "spill state shape mismatch".to_string(),
        ));
    }

    for (t, o) in target.iter_mut().zip(other.iter()) {
        match (t, o) {
            (AggState::Count(a), AggState::Count(b)) => *a += *b,
            (AggState::SumInt(a), AggState::SumInt(b)) => *a += *b,
            (AggState::SumFloat(a), AggState::SumFloat(b)) => *a += *b,
            (AggState::Min(a), AggState::Min(b)) => {
                if let Some(bv) = b {
                    if a.as_ref()
                        .map(|av| scalar_lt(bv, av))
                        .transpose()?
                        .unwrap_or(true)
                    {
                        *a = Some(bv.clone());
                    }
                }
            }
            (AggState::Max(a), AggState::Max(b)) => {
                if let Some(bv) = b {
                    if a.as_ref()
                        .map(|av| scalar_gt(bv, av))
                        .transpose()?
                        .unwrap_or(true)
                    {
                        *a = Some(bv.clone());
                    }
                }
            }
            (
                AggState::Avg {
                    sum: asum,
                    count: acount,
                },
                AggState::Avg {
                    sum: bsum,
                    count: bcount,
                },
            ) => {
                *asum += *bsum;
                *acount += *bcount;
            }
            _ => return Err(FfqError::Execution("spill state type mismatch".to_string())),
        }
    }

    Ok(())
}

fn estimate_groups_bytes(groups: &HashMap<Vec<ScalarValue>, Vec<AggState>>) -> usize {
    let mut total = 0_usize;
    for (k, v) in groups {
        total += 96;
        total += k.iter().map(scalar_estimate_bytes).sum::<usize>();
        total += v.iter().map(agg_state_estimate_bytes).sum::<usize>();
    }
    total
}

fn scalar_estimate_bytes(v: &ScalarValue) -> usize {
    match v {
        ScalarValue::Int64(_) => 8,
        ScalarValue::Float64Bits(_) => 8,
        ScalarValue::Utf8(s) => s.len(),
        ScalarValue::Boolean(_) => 1,
        ScalarValue::Null => 0,
    }
}

fn agg_state_estimate_bytes(v: &AggState) -> usize {
    match v {
        AggState::Count(_) => 8,
        AggState::SumInt(_) => 8,
        AggState::SumFloat(_) => 8,
        AggState::Min(x) | AggState::Max(x) => x.as_ref().map_or(0, scalar_estimate_bytes),
        AggState::Avg { .. } => 16,
    }
}

fn avg_count_col_name(name: &str) -> String {
    format!("__ffq_avg_count_{name}")
}

fn scalar_from_array(array: &ArrayRef, row: usize) -> Result<ScalarValue> {
    if array.is_null(row) {
        return Ok(ScalarValue::Null);
    }

    match array.data_type() {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FfqError::Execution("expected Int64Array".to_string()))?;
            Ok(ScalarValue::Int64(a.value(row)))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| FfqError::Execution("expected Float64Array".to_string()))?;
            Ok(ScalarValue::Float64Bits(a.value(row).to_bits()))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| FfqError::Execution("expected StringArray".to_string()))?;
            Ok(ScalarValue::Utf8(a.value(row).to_string()))
        }
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| FfqError::Execution("expected BooleanArray".to_string()))?;
            Ok(ScalarValue::Boolean(a.value(row)))
        }
        other => Err(FfqError::Unsupported(format!(
            "aggregate scalar type not supported yet: {other:?}"
        ))),
    }
}

fn scalars_to_array(values: &[ScalarValue], dt: &DataType) -> Result<ArrayRef> {
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int64(x) => b.append_value(*x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Int64 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Float64Bits(x) => b.append_value(f64::from_bits(*x)),
                    ScalarValue::Int64(x) => b.append_value(*x as f64),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Float64 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 8);
            for v in values {
                match v {
                    ScalarValue::Utf8(x) => b.append_value(x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Utf8 array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Boolean(x) => b.append_value(*x),
                    ScalarValue::Null => b.append_null(),
                    _ => {
                        return Err(FfqError::Execution(
                            "type mismatch while building Boolean array".to_string(),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(FfqError::Unsupported(format!(
            "aggregate output type not supported yet: {other:?}"
        ))),
    }
}

fn as_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(x) => Some(*x as f64),
        ScalarValue::Float64Bits(x) => Some(f64::from_bits(*x)),
        _ => None,
    }
}

fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> Result<bool> {
    match (a, b) {
        (ScalarValue::Int64(x), ScalarValue::Int64(y)) => Ok(x < y),
        (ScalarValue::Float64Bits(x), ScalarValue::Float64Bits(y)) => {
            Ok(f64::from_bits(*x) < f64::from_bits(*y))
        }
        (ScalarValue::Utf8(x), ScalarValue::Utf8(y)) => Ok(x < y),
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => Ok((!*x) & *y),
        _ => Err(FfqError::Execution(
            "cannot compare values of different types".to_string(),
        )),
    }
}

fn scalar_gt(a: &ScalarValue, b: &ScalarValue) -> Result<bool> {
    scalar_lt(b, a)
}

#[cfg(feature = "distributed")]
use ffq_distributed::DistributedRuntime as InnerDistributedRuntime;

#[cfg(feature = "distributed")]
#[derive(Debug, Default)]
pub struct DistributedRuntime {
    _inner: InnerDistributedRuntime,
}

#[cfg(feature = "distributed")]
impl DistributedRuntime {
    pub fn new() -> Self {
        Self {
            _inner: InnerDistributedRuntime::default(),
        }
    }
}

#[cfg(feature = "distributed")]
impl Runtime for DistributedRuntime {
    fn execute(
        &self,
        _plan: PhysicalPlan,
        _ctx: QueryContext,
        _catalog: Arc<Catalog>,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        async move {
            // v1 skeleton: distributed implementation will:
            // - serialize plan
            // - submit to coordinator
            // - stream results back
            let stream =
                futures::stream::empty::<Result<arrow::record_batch::RecordBatch>>().boxed();
            Ok(stream)
        }
        .boxed()
    }
}

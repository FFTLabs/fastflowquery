# Extensibility (v2)

- Status: draft
- Owner: @ffq-api
- Last Verified Commit: 7888e4c
- Last Verified Date: 2026-02-21

## Scope

This page defines the v2 extension contract for:

1. custom optimizer rules
2. scalar UDFs
3. custom physical operators

It also documents registration lifecycle and distributed-runtime behavior.

Primary code references:

1. `crates/client/src/engine.rs`
2. `crates/client/src/planner_facade.rs`
3. `crates/planner/src/optimizer.rs`
4. `crates/execution/src/udf.rs`
5. `crates/execution/src/physical_registry.rs`
6. `crates/distributed/src/worker.rs`
7. `crates/distributed/src/coordinator.rs`

## Extension Points Overview

`Engine` exposes the extension API:

1. optimizer rules:
   - `register_optimizer_rule`
   - `deregister_optimizer_rule`
2. scalar UDFs:
   - `register_scalar_udf`
   - `register_numeric_udf_type`
   - `deregister_scalar_udf`
3. physical operators:
   - `register_physical_operator_factory`
   - `deregister_physical_operator_factory`
   - `list_physical_operator_factories`

Registration return value semantics:

1. `false`: new name inserted
2. `true`: existing registration with same name replaced

## Lifecycle and Contracts

### Optimizer Rule Contract

Trait: `ffq_planner::OptimizerRule`.

Required methods:

1. `name() -> &str`
2. `rewrite(plan, ctx, cfg) -> Result<LogicalPlan>`

Behavior contract:

1. rules run after built-in optimizer passes
2. custom rules execute in deterministic lexical order by rule name
3. rule must preserve logical correctness (fallback to original shape when preconditions fail)

### Scalar UDF Contract

Trait: `ffq_execution::ScalarUdf`.

Required methods:

1. `name() -> &str`
2. `return_type(arg_types) -> Result<DataType>`
3. `invoke(args) -> Result<ArrayRef>`

Behavior contract:

1. `name` is normalized to lowercase during registration
2. `return_type` is used by analyzer/planner type checking
3. `invoke` is batch-wise Arrow-array execution
4. both planner and execution registries are updated by `Engine::register_scalar_udf`

### Physical Operator Contract

Trait: `ffq_execution::PhysicalOperatorFactory`.

Required methods:

1. `name() -> &str`
2. `execute(input_schema, input_batches, config) -> Result<(SchemaRef, Vec<RecordBatch>)>`

Behavior contract:

1. `PhysicalPlan::Custom.op_name` must match a registered factory name
2. `config` is string key/value and validated by factory implementation
3. output schema/batches must be self-consistent

## Embedded vs Distributed Behavior

### Embedded runtime

1. custom factory lookup is resolved from the engine's physical operator registry
2. if missing, query fails with unsupported/custom-operator error

### Distributed runtime

1. worker sends heartbeat capability list from `global_physical_operator_registry().names()`
2. coordinator assigns custom-op tasks only to workers advertising required op names
3. worker executes `PhysicalPlan::Custom` by looking up the factory in its local registry
4. if factory is missing on worker, task fails with clear unsupported error

Important operational rule:

1. factory registration is process-local
2. in multi-process deployments, each worker process must register the same custom factories at startup

See also:

1. `docs/v2/control-plane.md`
2. `docs/v2/distributed-runtime.md`
3. `docs/v2/custom-operators-deployment.md`

## Bootstrap Guidance

Recommended startup order:

1. build `Engine`
2. register optimizer rules
3. register scalar UDFs
4. register physical operator factories
5. register tables/catalog
6. execute queries

Distributed bootstrap additions:

1. register physical factories inside worker process bootstrap before poll loop starts
2. verify worker heartbeat advertises expected capability names
3. fail startup if required extension set is incomplete

For a full production rollout checklist, see `docs/v2/custom-operators-deployment.md`.

## Example 1: `my_add` Scalar UDF

The following shape matches `crates/client/tests/udf_api.rs`.

```rust
use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::kernels::numeric::add;
use arrow_schema::DataType;
use ffq_client::{Engine, ScalarUdf};

struct MyAddUdf;

impl ScalarUdf for MyAddUdf {
    fn name(&self) -> &str { "my_add" }

    fn return_type(&self, arg_types: &[DataType]) -> ffq_common::Result<DataType> {
        match arg_types {
            [DataType::Int64, DataType::Int64] => Ok(DataType::Int64),
            _ => Err(ffq_common::FfqError::Planning("my_add expects (Int64, Int64)".into())),
        }
    }

    fn invoke(&self, args: &[ArrayRef]) -> ffq_common::Result<ArrayRef> {
        let a = args[0].as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ffq_common::FfqError::Execution("arg0 not Int64".into())
        })?;
        let b = args[1].as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ffq_common::FfqError::Execution("arg1 not Int64".into())
        })?;
        Ok(Arc::new(add(a, b).map_err(|e| {
            ffq_common::FfqError::Execution(format!("my_add failed: {e}"))
        })?))
    }
}

# fn demo(engine: &Engine) -> ffq_common::Result<()> {
engine.register_scalar_udf(Arc::new(MyAddUdf));
let _df = engine.sql("SELECT my_add(l_orderkey, 3) FROM lineitem LIMIT 1")?;
# Ok(())
# }
```

Verification command:

```bash
cargo test -p ffq-client --test udf_api
```

## Example 2: Custom Optimizer Rule (`x > 10` -> `x >= 11`)

Reference implementation: `crates/planner/tests/optimizer_custom_rule.rs`.

```rust
use std::sync::Arc;
use ffq_planner::{BinaryOp, Expr, LogicalPlan, OptimizerRule, OptimizerConfig, OptimizerContext};

struct GtToGte11Rule;

impl OptimizerRule for GtToGte11Rule {
    fn name(&self) -> &str { "test_gt_to_gte_11" }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _ctx: &dyn OptimizerContext,
        _cfg: OptimizerConfig,
    ) -> ffq_common::Result<LogicalPlan> {
        // Traverse and rewrite BinaryOp(Gt, Int64(10)) -> BinaryOp(GtEq, Int64(11)).
        # Ok(plan)
    }
}

# fn register(engine: &ffq_client::Engine, rule: Arc<dyn OptimizerRule>) {
engine.register_optimizer_rule(rule);
# }
```

Verification command:

```bash
cargo test -p ffq-planner --test optimizer_custom_rule
```

## Example 3: Custom Physical Operator (`add_const_i64`)

This is the same pattern used in distributed tests (`crates/distributed/src/worker.rs`).

```rust
use std::collections::HashMap;
use std::sync::Arc;
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_client::PhysicalOperatorFactory;

struct AddConstFactory;

impl PhysicalOperatorFactory for AddConstFactory {
    fn name(&self) -> &str { "add_const_i64" }

    fn execute(
        &self,
        input_schema: SchemaRef,
        input_batches: Vec<RecordBatch>,
        config: &HashMap<String, String>,
    ) -> ffq_common::Result<(SchemaRef, Vec<RecordBatch>)> {
        // Read config keys: column, addend
        // Mutate selected Int64 column by +addend across all batches.
        # let _ = (input_schema.clone(), input_batches, config);
        # Ok((input_schema, Vec::new()))
    }
}

# fn register(engine: &ffq_client::Engine) {
engine.register_physical_operator_factory(Arc::new(AddConstFactory));
# }
```

Distributed requirement:

1. register this factory in every worker process (or via global worker bootstrap)
2. otherwise capability filtering prevents assignment or worker execution fails if scheduled without registry parity

Verification commands:

```bash
cargo test -p ffq-client --test physical_registry
cargo test -p ffq-distributed --features grpc coordinator_with_workers_executes_custom_operator_stage
```

## Failure Semantics

### Optimizer rules

1. rule rewrite errors surface as planning failures
2. a bad rule can invalidate planning for all queries in that engine session

### Scalar UDF

1. return-type mismatch errors are planning failures
2. array/type mismatch in `invoke` are execution failures

### Physical operators

1. missing factory registration is `Unsupported`
2. bad config parsing is `InvalidConfig`
3. array/schema misuse is `Execution`

## Troubleshooting

1. UDF callable not found:
   - ensure `register_scalar_udf` ran before query planning
2. custom rule not applied:
   - verify rule name registration and inspect `df.explain()` output
3. custom operator never scheduled in distributed:
   - verify workers advertise capability name through heartbeat
4. custom operator fails on worker:
   - ensure factory is registered in worker process, not only client process
5. extension replacement surprises:
   - check boolean return from register calls (`true` means replaced existing)

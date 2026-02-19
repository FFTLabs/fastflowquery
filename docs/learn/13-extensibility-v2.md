# LEARN-13: Extensibility in v2 (Rules, UDFs, Custom Operators)

This chapter explains how FFQ v2 extensibility works end-to-end: where extensions plug in, lifecycle guarantees, and distributed deployment realities.

## 1) Extension Points

`Engine` exposes three extension families:

1. optimizer rules
2. scalar UDFs
3. physical operator factories

Registration APIs:

1. `register_optimizer_rule` / `deregister_optimizer_rule`
2. `register_scalar_udf` / `deregister_scalar_udf`
3. `register_physical_operator_factory` / `deregister_physical_operator_factory`

## 2) Optimizer Rules

Contract trait: `ffq_planner::OptimizerRule`.

Key guarantees:

1. custom rules run after built-in passes
2. custom rule order is deterministic by rule name
3. rule rewrite must preserve logical correctness

Example pattern:

1. test rule rewrites `x > 10` to `x >= 11`
2. reference: `crates/planner/tests/optimizer_custom_rule.rs`

Runnable check:

```bash
cargo test -p ffq-planner --test optimizer_custom_rule
```

## 3) Scalar UDFs

Contract trait: `ffq_execution::ScalarUdf`.

Required methods:

1. `name`
2. `return_type`
3. `invoke`

Runtime behavior:

1. name is normalized to lowercase
2. planner uses resolver for type-checking
3. execution invokes batch-wise Arrow arrays

Example pattern:

1. `my_add(col, 3)` UDF
2. reference: `crates/client/tests/udf_api.rs`

Runnable check:

```bash
cargo test -p ffq-client --test udf_api
```

## 4) Custom Physical Operators

Contract trait: `ffq_execution::PhysicalOperatorFactory`.

Factory does:

1. identify operator name (`name()`)
2. execute transformation over materialized input batches (`execute(...)`)

Example:

1. `add_const_i64` custom op factory
2. references:
   - `crates/client/tests/physical_registry.rs`
   - `crates/distributed/src/worker.rs` (custom-op stage test)

Runnable checks:

```bash
cargo test -p ffq-client --test physical_registry
cargo test -p ffq-distributed --features grpc coordinator_with_workers_executes_custom_operator_stage
```

## 5) Embedded vs Distributed Behavior

### Embedded

1. engine-local physical operator registry is used during execution
2. missing custom-op factory yields unsupported execution error

### Distributed

1. worker advertises capability names from global registry in heartbeat
2. coordinator routes custom-op tasks only to workers with required capabilities
3. worker executes custom op by local registry lookup
4. missing factory on worker fails task explicitly

Runnable capability checks:

```bash
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

## 6) Bootstrap Guidance (Production)

Because factory registration is process-local:

1. register custom factories in every worker process at startup
2. verify heartbeat capability list includes expected names
3. only then allow queries requiring those operators

Reference deployment contract:

1. `docs/v2/custom-operators-deployment.md`

## 7) Failure Modes to Understand

1. custom-op task never assigned:
   - no worker advertises required capability
2. task assigned but execution fails:
   - worker registry missing operator implementation
3. partial rollout:
   - only subset of workers can run operator; throughput drops/stalls

## 8) Code References

1. `crates/client/src/engine.rs`
2. `crates/planner/src/optimizer.rs`
3. `crates/execution/src/udf.rs`
4. `crates/execution/src/physical_registry.rs`
5. `crates/distributed/src/coordinator.rs`
6. `crates/distributed/src/worker.rs`

## 9) Why This Design Works

1. planner/execution extension points are explicit and testable
2. registration lifecycle is simple and deterministic
3. capability-aware distributed routing preserves correctness for custom semantics
4. process-local bootstrap makes operational responsibility explicit

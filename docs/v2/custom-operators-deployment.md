# Custom Operators Deployment Contract (v2)

- Status: draft
- Owner: @ffq-runtime
- Last Verified Commit: TBD
- Last Verified Date: TBD

## Scope

This page defines production deployment rules for custom physical operators in distributed mode.

It covers:

1. static/bootstrap registration model
2. capability advertisement from workers
3. coordinator routing behavior
4. verification checklist
5. mismatch and failure modes

Core implementation references:

1. `crates/execution/src/physical_registry.rs`
2. `crates/distributed/src/worker.rs`
3. `crates/distributed/src/coordinator.rs`
4. `crates/distributed/proto/ffq_distributed.proto`

## Runtime Contract

Custom operator registration is process-local.

1. each worker process has its own in-memory physical operator registry
2. registration in client/coordinator process does not automatically register factories in workers
3. workers advertise available custom operator names via heartbeat payload

Capability source on worker:

1. `global_physical_operator_registry().names()`

Heartbeat payload field:

1. `HeartbeatRequest.custom_operator_capabilities`

Coordinator assignment rule:

1. tasks requiring custom operators are assigned only to workers advertising all required op names
2. if no worker matches, tasks remain queued until a capable worker appears

## Bootstrap Model (Static Linked-In)

Recommended production model for v2:

1. compile workers with required custom factories linked in
2. register factories during worker startup bootstrap
3. start poll loop only after bootstrap succeeds

Pseudo bootstrap sequence:

1. initialize runtime/config
2. call `register_global_physical_operator_factory(...)` for each required factory
3. assert registry contains required names
4. start worker (`Worker::new` + poll loop)

This avoids runtime drift where some workers lack operator support.

## Coordinator/Worker Boot Checklist

### Worker boot checklist

1. required operator factories registered at process startup
2. registry names validated against expected deployment list
3. worker heartbeat seen by coordinator
4. heartbeat includes expected `custom_operator_capabilities`

### Coordinator checklist

1. `GetTask` filtering is enabled (default behavior)
2. task assignments for `PhysicalPlan::Custom` include required op names
3. no fallback path assigns custom-op tasks to incapable workers

### Query rollout checklist

1. submit known custom-op query in staging
2. verify assignment goes only to capable workers
3. verify query succeeds and output is correct
4. verify failure signal is clear when capability set is incomplete

## Capability Verification Commands

Scheduler/capability unit checks:

```bash
cargo test -p ffq-distributed --features grpc coordinator_assigns_custom_operator_tasks_only_to_capable_workers
```

End-to-end custom-op distributed execution:

```bash
cargo test -p ffq-distributed --features grpc coordinator_with_workers_executes_custom_operator_stage
```

Expected:

1. assignment is restricted to workers with required capability names
2. custom operator stage reaches succeeded state when all workers are bootstrapped correctly

## Mismatch Failure Modes

### Mode A: No worker advertises required capability

Symptoms:

1. custom-op task remains queued
2. query does not make progress to terminal success

Action:

1. verify bootstrap registration ran in worker processes
2. verify heartbeat payload includes required name

### Mode B: Worker receives custom-op task but factory missing at execution

Symptoms:

1. task fails with unsupported error:
   - `custom physical operator '<name>' is not registered on worker`
2. retry/blacklist behavior may trigger depending on policy

Action:

1. ensure registration uses the same operator name as plan `op_name`
2. ensure worker image/build includes factory code and bootstrap registration

### Mode C: Partial fleet rollout (some workers upgraded, some not)

Symptoms:

1. capable workers execute tasks; incapable workers stay idle for custom-op tasks
2. throughput degradation or stalled progress if capable capacity too low

Action:

1. complete rolling update before enabling queries requiring new operator
2. temporarily reduce query load or worker concurrency caps to match capable pool

## Operational Recommendations

1. keep a single source-of-truth list of required custom operators per deployment
2. validate worker capability sets at startup and in health checks
3. gate production query rollout on passing custom-op distributed test/smoke
4. alert on long-lived queued custom-op tasks (capability mismatch indicator)

## Related Docs

1. `docs/v2/extensibility.md`
2. `docs/v2/control-plane.md`
3. `docs/v2/distributed-runtime.md`

# Observability and Debugging by Signal

This chapter explains how to debug FFQ using runtime signals instead of guesswork: traces, metrics, and profiling hooks.

## 1) Signal model: what to observe first

Use this order for triage:

1. Traces/logs for *where* failure or slowness is happening.
2. Metrics for *how much* pressure exists (rows/bytes/time/spill/shuffle/scheduler).
3. Profiling for *why* CPU time is concentrated in specific operators.

The core correlation keys are:

1. `query_id`
2. `stage_id`
3. `task_id`
4. `operator`

These appear in spans and metric labels, allowing cross-signal joins.

## 2) Tracing: execution timeline and context

Primary execution span:

1. `operator_execute`

Attached fields:

1. `query_id`
2. `stage_id`
3. `task_id`
4. `operator`

Where this is emitted:

1. Embedded operator evaluation in `crates/client/src/runtime.rs`
2. Distributed worker execution in `crates/distributed/src/worker.rs`

What tracing is best for:

1. Identifying failing operator/stage quickly.
2. Reconstructing task lifecycle (assignment/start/finish).
3. Correlating retries and map-output registration with coordinator events.

## 3) Metrics: quantitative pressure map

Metrics are registered in `crates/common/src/metrics.rs` and exported as Prometheus text.

### Operator metrics

Labels: `query_id`, `stage_id`, `task_id`, `operator`

1. `ffq_operator_rows_in_total`
2. `ffq_operator_rows_out_total`
3. `ffq_operator_batches_in_total`
4. `ffq_operator_batches_out_total`
5. `ffq_operator_bytes_in_total`
6. `ffq_operator_bytes_out_total`
7. `ffq_operator_time_seconds` (histogram)

Interpretation:

1. High `rows_in` + low `rows_out` = selective operator (expected for filters).
2. High operator time with moderate row volume = potentially expensive expression/join path.

### Shuffle metrics

Labels: `query_id`, `stage_id`, `task_id`

1. `ffq_shuffle_bytes_written_total`
2. `ffq_shuffle_bytes_read_total`
3. `ffq_shuffle_partitions_written_total`
4. `ffq_shuffle_partitions_read_total`
5. `ffq_shuffle_fetch_seconds` (histogram)

Interpretation:

1. Large shuffle bytes + long fetch time indicates network/disk movement bottleneck.
2. Partition counts help validate stage fanout and partitioning behavior.

### Spill metrics

Labels: `query_id`, `stage_id`, `task_id`, `kind`

1. `ffq_spill_bytes_total`
2. `ffq_spill_time_seconds` (histogram)

Interpretation:

1. Non-zero spill bytes means memory budget path activated.
2. High spill time often correlates with degraded join/aggregate latency.

### Scheduler metrics

Gauges (`query_id`, `stage_id`):

1. `ffq_scheduler_queued_tasks`
2. `ffq_scheduler_running_tasks`

Counter (`query_id`, `stage_id`):

1. `ffq_scheduler_retries_total`

Interpretation:

1. High queued + low running means scheduling/resource saturation.
2. Rising retries indicates instability (task failures, fetch errors, unhealthy workers).

## 4) Profiling hooks: pinpoint hot code paths

Feature flag:

1. `profiling`

What it enables:

1. `/metrics` HTTP exporter via `ffq_common::run_metrics_exporter`.
2. Flamegraph-friendly instrumentation using:
   - `#[cfg_attr(feature = "profiling", inline(never))]`
   - explicit spans (`profile_hash_join`, `profile_grace_hash_join`, `profile_hash_aggregate`, `profile_topk_by_score`)

Why this works:

1. `inline(never)` keeps hot functions visible in sampling profiles.
2. Profiling spans let you separate CPU cost by operator family.

## 5) Embedded debugging playbook

When a query is slow/failing in embedded mode:

1. Capture `operator_execute` spans and identify last successful operator.
2. Inspect operator metrics for that query:
   - time histogram hotspot,
   - rows/bytes explosion,
   - spill activation.
3. If spill is high, increase memory budget and compare behavior.
4. If still CPU-bound, run with `profiling` and collect flamegraph to inspect hot kernels/operators.

## 6) Distributed debugging playbook

When a distributed query regresses or fails:

1. Use `query_id` to correlate coordinator logs, worker logs, and metrics.
2. Check scheduler gauges/counters first:
   - queued vs running,
   - retries trend.
3. Check shuffle metrics next:
   - bytes written/read by stage,
   - fetch latency spikes.
4. Validate stage/task failure point from traces (`stage_id`, `task_id`, `operator`).
5. Inspect spill metrics for worker memory pressure on join/aggregate stages.

## 7) Symptom -> likely signal pattern

1. **Symptom:** query hangs in distributed mode.
   - Signals: `queued_tasks` high, `running_tasks` low, no operator progress.
   - Likely area: worker availability/scheduling loop.
2. **Symptom:** join stage is much slower than expected.
   - Signals: high `spill_bytes_total`, high join operator time, rising shuffle bytes.
   - Likely area: memory budget too low or skewed join distribution.
3. **Symptom:** result latency dominated by exchange boundaries.
   - Signals: high `shuffle_fetch_seconds`, high shuffle bytes, moderate compute time.
   - Likely area: shuffle IO/network path.
4. **Symptom:** repeated stage failures/retries.
   - Signals: `scheduler_retries_total` increasing, repeated task status errors.
   - Likely area: flaky worker execution or bad stage input assumptions.

## 8) `/metrics` usage paths

Without `profiling`:

1. In-process scrape text through `Engine::prometheus_metrics()`.

With `profiling`:

1. Expose HTTP endpoint using `Engine::serve_metrics_exporter(addr)`.
2. Scrape `GET /metrics` from Prometheus or `curl`.

## 9) Practical caveats in v1

1. Metrics registry is process-global singleton.
2. Label cardinality includes query/stage/task IDs; long retention may create high series counts.
3. Histogram bucket strategy uses default prometheus histogram behavior in v1.
4. Signal coverage is strongest around operators/shuffle/scheduler; some control-plane details remain basic.

## 10) Reference map

1. `docs/v1/observability.md`
2. `crates/common/src/metrics.rs`
3. `crates/common/src/metrics_exporter.rs`
4. `crates/client/src/runtime.rs`
5. `crates/distributed/src/worker.rs`
6. `crates/client/src/engine.rs`

## Runnable command

```bash
cargo test -p ffq-common --features profiling metrics_handler_returns_prometheus_text
```

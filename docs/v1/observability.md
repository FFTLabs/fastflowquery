# Observability Guide

This document describes FFQ v1 observability as implemented: tracing fields, Prometheus metrics, profiling hooks, and `/metrics` exporter usage.

## Tracing

FFQ uses `tracing` spans and structured events in embedded and distributed execution paths.

## Required trace fields

The execution operator span includes:

1. `query_id`
2. `stage_id`
3. `task_id`
4. `operator`

Primary span:

1. `operator_execute`

Where it is attached:

1. Embedded runtime operator evaluation (`crates/client/src/runtime.rs`)
2. Distributed worker stage/operator evaluation (`crates/distributed/src/worker.rs`)

Additional coordinator/worker events include the same IDs when available (task assignment, task start/finish, status transitions), plus operation-specific fields like `attempt` and `worker_id`.

## Structured logs

Events are emitted with key-value fields, for example:

1. query start/end in embedded runtime (`mode`, `rows`, `batches`)
2. distributed submit/poll/terminal events (`endpoint`, status message)
3. coordinator scheduling and task status updates (`operator` values like `CoordinatorSubmit`, `CoordinatorGetTask`, `CoordinatorReportTaskStatus`)

Log formatting (JSON vs text) depends on your tracing subscriber setup in the host process.

## Prometheus Metrics

Metrics are registered in `crates/common/src/metrics.rs` and exported in Prometheus text format.

## Operator metrics (labels: `query_id`, `stage_id`, `task_id`, `operator`)

1. `ffq_operator_rows_in_total`
2. `ffq_operator_rows_out_total`
3. `ffq_operator_batches_in_total`
4. `ffq_operator_batches_out_total`
5. `ffq_operator_bytes_in_total`
6. `ffq_operator_bytes_out_total`
7. `ffq_operator_time_seconds` (histogram)

## Shuffle metrics (labels: `query_id`, `stage_id`, `task_id`)

1. `ffq_shuffle_bytes_written_total`
2. `ffq_shuffle_bytes_read_total`
3. `ffq_shuffle_partitions_written_total`
4. `ffq_shuffle_partitions_read_total`
5. `ffq_shuffle_fetch_seconds` (histogram; used for shuffle write/read timing)

## Spill metrics (labels: `query_id`, `stage_id`, `task_id`, `kind`)

1. `ffq_spill_bytes_total`
2. `ffq_spill_time_seconds` (histogram)

## Scheduler metrics

Gauge labels: `query_id`, `stage_id`

1. `ffq_scheduler_queued_tasks`
2. `ffq_scheduler_running_tasks`

Counter labels: `query_id`, `stage_id`

1. `ffq_scheduler_retries_total`

## Feature `profiling`

`profiling` adds two key capabilities:

1. HTTP metrics exporter (`/metrics`) via `ffq_common::run_metrics_exporter`.
2. Flamegraph-friendly hooks in hot operators:
   - `#[cfg_attr(feature = "profiling", inline(never))]`
   - profiling spans like `profile_topk_by_score`, `profile_hash_join`, `profile_grace_hash_join`, `profile_hash_aggregate`

Without `profiling`, metrics are still collected in-process and can be retrieved as text via:

1. `Engine::prometheus_metrics()`

## `/metrics` Exporter Usage

Enable feature and start exporter:

```rust
use std::net::SocketAddr;
use ffq_client::Engine;
use ffq_common::EngineConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(EngineConfig::default())?;
    let addr: SocketAddr = "127.0.0.1:9101".parse()?;
    engine.serve_metrics_exporter(addr).await?;
    Ok(())
}
```

Build/run with:

```bash
cargo run -p ffq-client --features profiling
```

Manual check:

```bash
curl -s http://127.0.0.1:9101/metrics | head
```

## Prometheus scrape example

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: ffq
    static_configs:
      - targets: ["127.0.0.1:9101"]
    metrics_path: /metrics
```

## Interpreting key metrics

1. Operator throughput:
   - `rate(ffq_operator_rows_out_total[1m])` by `operator` shows rows/sec per operator.
2. Operator selectivity:
   - compare `rows_out_total` vs `rows_in_total` for filters/joins.
3. Operator CPU/latency hotspots:
   - use `ffq_operator_time_seconds` histogram quantiles by operator.
4. Shuffle pressure:
   - high `ffq_shuffle_bytes_written_total` and `ffq_shuffle_fetch_seconds` indicates data-movement bottlenecks.
5. Spill pressure:
   - non-zero or growing `ffq_spill_bytes_total` indicates memory pressure and spill path usage.
6. Scheduler backpressure:
   - sustained high `ffq_scheduler_queued_tasks` with low `ffq_scheduler_running_tasks` suggests slot starvation or blacklisted/slow workers.
7. Retry instability:
   - increasing `ffq_scheduler_retries_total` indicates task failures/retries; correlate with worker logs and shuffle fetch errors.

## Notes and v1 caveats

1. Metrics are process-global (`global_metrics()` singleton).
2. Label cardinality includes `query_id`/`stage_id`/`task_id`; keep retention windows reasonable in long-running dev clusters.
3. Histogram bucket configuration currently uses Prometheus defaults.

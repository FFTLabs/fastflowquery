# Missing Docs Rollout Backlog

This tracks incremental `#![warn(missing_docs)]` rollout across crates.

## Enabled now

- `ffq-client`
  - status: `deny(missing_docs)` in `crates/client/src/lib.rs`
  - current gate: `cargo check -p ffq-client` clean for `missing_docs`
- `ffq-common`
  - status: `deny(missing_docs)` in `crates/common/src/lib.rs`
  - current gate: `cargo rustc -p ffq-common --lib -- -D missing-docs` clean
- `ffq-storage`
  - status: `deny(missing_docs)` in `crates/storage/src/lib.rs`
  - current gate: `cargo rustc -p ffq-storage --lib -- -D missing-docs` clean
- `ffq-planner`
  - status: `deny(missing_docs)` in `crates/planner/src/lib.rs`
  - current gate: `cargo rustc -p ffq-planner --lib -- -D missing-docs` clean
- `ffq-execution`
  - status: `deny(missing_docs)` in `crates/execution/src/lib.rs`
  - current gate: `cargo rustc -p ffq-execution --lib -- -D missing-docs` clean
- `ffq-distributed`
  - status: `deny(missing_docs)` in `crates/distributed/src/lib.rs`
  - current gate: `cargo rustc -p ffq-distributed --lib -- -D missing-docs` clean
- `ffq-shuffle`
  - status: `deny(missing_docs)` in `crates/shuffle/src/lib.rs`
  - current gate: `cargo rustc -p ffq-shuffle --lib -- -D missing-docs` clean
- `ffq-sql`
  - status: `deny(missing_docs)` in `crates/sql/src/lib.rs`
  - current gate: `cargo rustc -p ffq-sql --lib -- -D missing-docs` clean

## Next crates

No additional crates are currently queued in this backlog.

## Rollout command pattern

Use this per crate:

```bash
cargo check -p <crate>
```

When all crates above are clean, optionally escalate lint strength from
`warn(missing_docs)` to `deny(missing_docs)` crate-by-crate.

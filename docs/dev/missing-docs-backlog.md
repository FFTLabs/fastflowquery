# Missing Docs Rollout Backlog

This tracks incremental `#![warn(missing_docs)]` rollout across crates.

## Enabled now

- `ffq-client`
  - status: enabled in `crates/client/src/lib.rs`
  - current gate: `cargo check -p ffq-client` clean for `missing_docs`

## Next crates

1. `ffq-storage`
- reason: user-facing contracts (`TableDef`, providers, schema inference)
- action: enable in `crates/storage/src/lib.rs`, fix warnings

2. `ffq-planner`
- reason: logical/physical planning types are public and heavily reused
- action: enable in `crates/planner/src/lib.rs`, fix warnings

3. `ffq-execution`
- reason: execution traits/streams are shared runtime contracts
- action: enable in `crates/execution/src/lib.rs`, fix warnings

4. `ffq-distributed`
- reason: public coordinator/worker APIs and RPC glue
- action: enable in `crates/distributed/src/lib.rs`, fix warnings

5. `ffq-common`
- reason: shared config/error/metrics public surface
- action: enable in `crates/common/src/lib.rs`, fix warnings

## Rollout command pattern

Use this per crate:

```bash
cargo check -p <crate>
```

When all crates above are clean, optionally escalate lint strength from
`warn(missing_docs)` to `deny(missing_docs)` crate-by-crate.

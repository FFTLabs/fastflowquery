# FFQ (FastFlowQuery)

This repository provides a library-first query engine with feature-gated optional components:
- distributed (gRPC coordinator/worker)
- vector (vector datatype + similarity kernels)
- qdrant (vector connector)
- s3 (object-store provider)

By default, `cargo build` builds `ffq-client` with the core embedded runtime surface.

## Documentation (Canonical)

Canonical docs entry for current work:

1. `docs/v2/README.md`
2. `docs/v2/quickstart.md` (first runnable path)
3. `docs/v2/testing.md` (validation checklist)

Documentation policy:

1. `docs/v2/*` is the source of truth for current behavior.
2. Any behavior/API/config change must update at least one relevant `docs/v2/*` page in the same change.

Archived v1 docs:

1. `docs/v1/README.md`

## Quick Start

Quick REPL start:

```bash
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json
```

Then run:

```sql
SELECT l_orderkey, l_quantity FROM lineitem LIMIT 5;
```

Full REPL reference:

1. `docs/v2/repl.md`

FFI (C ABI) reference:

1. `docs/v2/ffi-python.md`

Python bindings reference:

1. `docs/v2/ffi-python.md`

For a concept-first deep guide (architecture, optimizer, distributed control plane, labs, glossary, FAQ):

1. `docs/learn/README.md`

## Environment

- Copy `.env.example` to `.env` for local overrides.
- `ffq-client` loads `.env` automatically (best-effort) on session creation.
- For distributed mode, set `FFQ_COORDINATOR_ENDPOINT`, e.g. `http://127.0.0.1:50051`.

## License
Licensed under the Apache License, Version 2.0. See LICENSE.

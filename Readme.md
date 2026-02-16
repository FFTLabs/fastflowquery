# FFQ (FastFlowQuery) — Workspace Skeleton

This is a v1 repo skeleton with feature-gated optional components:
- distributed (gRPC coordinator/worker)
- vector (vector datatype + similarity kernels)
- qdrant (vector connector)
- s3 (object-store provider)

By default, `cargo build` builds the lightweight `ffq-client` crate (embedded-only).

## Environment

- Copy `.env.example` to `.env` for local overrides.
- `ffq-client` loads `.env` automatically (best-effort) on session creation.
- For distributed mode, set `FFQ_COORDINATOR_ENDPOINT`, e.g. `http://127.0.0.1:50051`.

## License
Licensed under the Apache License, Version 2.0. See LICENSE.

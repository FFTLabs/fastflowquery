# FFQ (FastFlowQuery) — Workspace Skeleton

This is a v1 repo skeleton with feature-gated optional components:
- distributed (gRPC coordinator/worker)
- vector (vector datatype + similarity kernels)
- qdrant (vector connector)
- s3 (object-store provider)

By default, `cargo build` builds the lightweight `ffq-client` crate (embedded-only).

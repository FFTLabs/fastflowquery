# API SemVer Policy (v2)

This project follows SemVer for its **public API**.

## Public API scope

For v2, the primary stable Rust surface is:

1. `ffq_client::Engine`
2. `ffq_client::DataFrame`

The contract includes (non-exhaustive):

1. `Engine::new`
2. `Engine::config`
3. `Engine::register_table` / `Engine::register_table_checked`
4. `Engine::sql` / `Engine::sql_with_params`
5. `DataFrame::collect_stream` / `DataFrame::collect`
6. Optional convenience API behind features:
   - `Engine::hybrid_search` (`vector`)

Items not documented as public/stable may change in minor releases.

## Versioning rules

1. **Patch (`x.y.Z`)**:
   - bug fixes only
   - no breaking changes to the public API
2. **Minor (`x.Y.z`)**:
   - additive API changes allowed
   - deprecations allowed
   - no breaking removals/signature changes
3. **Major (`X.y.z`)**:
   - breaking API changes allowed

## Deprecation policy

1. Deprecations are introduced in minor/patch releases with `#[deprecated]` and migration notes.
2. Deprecated APIs remain available until the next major release unless a security issue requires earlier removal.
3. Breaking removals and signature changes are only allowed in major releases.

## CI policy

1. Rustdoc must build cleanly for selected crates.
2. `cargo-semver-checks` runs on PRs for `ffq-client` against the base branch.
3. PRs that introduce unintended breaking changes fail CI.

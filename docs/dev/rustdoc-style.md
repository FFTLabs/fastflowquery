# Rustdoc Style Guide

This document defines source-level Rust documentation standards for FastFlowQuery.

## Scope

Use this guide for:
- crate-level docs (`//!`)
- module-level docs (`//!`)
- item-level docs (`///`) on public APIs

Goals:
- make public behavior and constraints explicit
- keep docs accurate and reviewable
- keep doc examples minimal and runnable where practical

## `//!` vs `///`

Use `//!` for:
- crate overviews (`lib.rs`, `main.rs`)
- module architecture notes
- cross-cutting contracts that apply to multiple items

Use `///` for:
- public structs, enums, traits, functions, type aliases, constants
- externally relevant behavior for non-public items when it materially reduces ambiguity

Rule of thumb:
- if the information is about a namespace/module, use `//!`
- if the information is about one item, use `///`

## Required Sections (when applicable)

For public functions/methods with fallible behavior:
- include `# Errors`
- describe expected error classes and triggering conditions

For functions with panics that are part of contract:
- include `# Panics`

For unsafe APIs:
- include `# Safety`

For performance-sensitive or non-obvious semantics:
- include `# Notes` or `# Invariants`

## Examples

Use `# Examples` for key public entry points.

Guidelines:
- keep examples short and focused
- prefer compile-checked examples
- if runtime setup is heavy, use `no_run`
- avoid stale pseudo-code

Example shape:

```rust
/// Executes a query and collects all batches.
///
/// # Errors
/// Returns an error if planning or execution fails.
///
/// # Examples
/// ```no_run
/// # use ffq_client::Engine;
/// # use ffq_common::EngineConfig;
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let engine = Engine::new(EngineConfig::default())?;
/// let batches = engine.sql("SELECT 1")?.collect().await?;
/// assert!(!batches.is_empty());
/// # Ok(())
/// # }
/// ```
/// ```

## Invariants and Contracts

Document invariants where violations can cause subtle bugs:
- schema assumptions (column order/type expectations)
- planner/optimizer preconditions
- runtime semantics (determinism, spill behavior, retry semantics)
- sink commit semantics and idempotency expectations

When possible, tie invariants to corresponding tests by file name.

## Intra-doc Links

Prefer intra-doc links for navigation:
- types: [`Engine`], [`DataFrame`]
- methods: [`Engine::sql`]
- modules: [`crate::runtime`]

Guidelines:
- link to canonical items instead of repeating full explanations
- avoid broken links by running `cargo doc` in CI/dev checks

## Tone and Style

- be precise and concrete
- avoid marketing language
- avoid duplicating implementation details that change frequently
- document behavior/contracts, not line-by-line code flow

## Review Checklist

When reviewing doc changes, verify:
- correct placement of `//!` vs `///`
- `# Errors` exists where needed
- examples compile or are explicitly `no_run`
- invariants and constraints are explicitly stated
- links resolve and docs reflect current behavior

# FFQ REPL Reference (v2 Bootstrap)

- Status: draft
- Owner: @ffq-docs
- Last Verified Commit: TBD
- Last Verified Date: TBD
- Source: inherited/adapted from prior version docs; v2 verification pending


This page is the complete bootstrap reference for `ffq-client repl` in v2.

## Start REPL

Minimal:

```bash
cargo run -p ffq-client -- repl
```

With catalog:

```bash
cargo run -p ffq-client -- repl \
  --catalog tests/fixtures/catalog/tables.json
```

With distributed endpoint:

```bash
cargo run -p ffq-client -- repl \
  --catalog tests/fixtures/catalog/tables.json \
  --coordinator-endpoint http://127.0.0.1:50051
```

## REPL CLI Flags

Supported flags:

1. `--catalog <PATH>`
2. `--coordinator-endpoint <URL>`
3. `--batch-size-rows <N>`
4. `--mem-budget-bytes <N>`
5. `--spill-dir <PATH>`
6. `--shuffle-partitions <N>`
7. `--broadcast-threshold-bytes <N>`
8. `--schema-inference off|on|strict|permissive`
9. `--schema-writeback true|false`
10. `--schema-drift-policy fail|refresh`

## Built-in Commands

Supported commands:

1. `\help`
2. `\q`
3. `\tables`
4. `\schema <table>`
5. `\plan on|off`
6. `\timing on|off`
7. `\mode table|csv|json`

Command behavior:

1. `\tables` prints currently registered table names.
2. `\schema <table>` prints schema fields and schema origin:
   - `catalog-defined`
   - `inferred`
3. `\plan on` prints logical plan before execution.
4. `\timing on` prints elapsed query time in ms.
5. `\mode` changes result rendering format.

## SQL Input Model

Input semantics:

1. SQL is accumulated until a terminating `;`.
2. Multi-line SQL is supported.
3. Empty lines are ignored.
4. `--` comment lines are ignored.
5. REPL commands (`\...`) are recognized only when not in the middle of a SQL statement.

Exit semantics:

1. `\q` exits immediately.
2. `Ctrl+D` exits.
3. `Ctrl+C` cancels current partial statement buffer.

## Output Modes

Modes:

1. `table` (default): Arrow pretty table.
2. `csv`: header + escaped rows.
3. `json`: pretty JSON array of row objects.

Switch mode:

```sql
\mode csv
SELECT l_orderkey FROM lineitem LIMIT 3;
\mode json
SELECT l_orderkey FROM lineitem LIMIT 3;
```

## Write Query UX

For `INSERT INTO ... SELECT ...` and sink-like queries:

1. If execution returns empty/zero-row sink batches, REPL prints `OK`.
2. For non-empty batch results, normal table/csv/json rendering is used.

## Error Taxonomy and Hints

REPL classifies errors into:

1. `planning`
2. `execution`
3. `config`
4. `io`
5. `unsupported`

Format:

```text
[<category>] <stage>: <error message>
hint: <actionable hint>
```

Schema-related messages:

1. `schema inference failed ...`
   - check parquet paths/permissions and file validity
2. `schema drift detected ...`
   - refresh policy recommended for mutable file sets
3. `incompatible parquet files ...`
   - ensure files in one table have compatible schema

## Schema Policy Usage

Recommended dev setup:

```bash
FFQ_SCHEMA_INFERENCE=on \
FFQ_SCHEMA_DRIFT_POLICY=refresh \
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tables.json
```

Recommended strict CI/repro setup:

```bash
FFQ_SCHEMA_INFERENCE=strict \
FFQ_SCHEMA_DRIFT_POLICY=fail \
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tables.json
```

Writeback setup:

```bash
FFQ_SCHEMA_WRITEBACK=true \
cargo run -p ffq-client -- repl --catalog tests/fixtures/catalog/tables.json
```

## Config Precedence

Effective runtime config precedence:

1. REPL CLI flags
2. Environment overrides loaded in session (`FFQ_*`)
3. `EngineConfig::default()`

Example:

1. `--schema-inference strict` on CLI overrides default inference behavior.
2. `FFQ_SCHEMA_DRIFT_POLICY=refresh` applies if not overridden by CLI-provided config.

## History and Line Editing

REPL uses `rustyline`:

1. arrow-key history navigation
2. editable current line
3. persistent history file: `~/.ffq_history`

## Smoke Validation

Interactive:

```bash
make repl
```

Non-interactive smoke:

```bash
make repl-smoke
```

## Troubleshooting

1. `unknown table: <name>`:
   - check `--catalog` path
   - run `\tables`
2. `table '<name>' has no schema`:
   - provide schema manually or enable inference
3. `connect coordinator failed`:
   - verify endpoint and cluster health
4. `schema drift detected`:
   - use `--schema-drift-policy refresh` for mutable files
5. `incompatible parquet files`:
   - align schemas or split table definitions

## Related Docs

1. Quick start: `docs/v2/quickstart.md`
2. Storage/catalog and schema inference: `docs/v2/storage-catalog.md`
3. Client runtime behavior: `docs/v2/client-runtime.md`
4. Integration runbook: `docs/v2/integration-13.2.md`

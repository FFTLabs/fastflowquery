# Lab 02: Distributed Run (Coordinator + 2 Workers)

Goal: run distributed integration against docker compose and confirm the same query suite executes through coordinator/worker flow.

## Prerequisites

1. Docker daemon running.
2. Ports `50051`, `50061`, `50062` available.
3. Embedded baseline already passes (`make test-13.2-embedded`).

## Steps

1. Start cluster:

```bash
docker compose -f docker/compose/ffq.yml up --build -d
```

2. Check service health:

```bash
docker compose -f docker/compose/ffq.yml ps
```

3. Run distributed integration test target:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 make test-13.2-distributed
```

4. Run full parity command (embedded + distributed + teardown):

```bash
make test-13.2-parity
```

5. When done, stop cluster:

```bash
docker compose -f docker/compose/ffq.yml down -v
```

## Expected Output

1. `docker compose ... ps` shows `coordinator`, `worker-1`, `worker-2` as healthy.
2. `make test-13.2-distributed` runs `integration_distributed` and passes.
3. `make test-13.2-parity` passes all checks and exits `0`.

## Troubleshooting

1. Endpoint not reachable:
   - confirm health with `docker compose ... ps`.
   - inspect logs: `docker compose -f docker/compose/ffq.yml logs -f coordinator worker-1 worker-2`.
2. Schema errors like `join key ... not found in schema`:
   - verify `tests/fixtures/catalog/tables.json` has schemas for distributed tables.
   - restart cluster after catalog edits.
3. Flaky temp artifacts:
   - keep distributed temp dir for inspection:
     - `FFQ_KEEP_INTEGRATION_TMP=1 make test-13.2-distributed`

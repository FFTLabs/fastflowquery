# Contributing

Thanks for your interest in contributing!

## Quick start
1. Fork the repo and create your branch from `main`.
2. Make your change.
3. Run tests / lint (if the project has them).
4. Open a Pull Request.

## Reporting bugs
Please include:
- What you expected to happen vs. what happened
- Steps to reproduce
- Version/OS/runtime info (as relevant)
- Logs/screenshots if helpful

## Suggesting enhancements
Open an issue describing:
- The problem you’re trying to solve
- Proposed solution / alternatives
- Any relevant examples or prior art

## Pull requests
- Keep PRs focused (one logical change).
- Add/update tests when behavior changes.
- Update docs/README if you change usage.
- Be respectful in review discussions.

Source-level Rust documentation standard:
- `docs/dev/rustdoc-style.md`

API SemVer + deprecation policy:
- `docs/dev/api-semver-policy.md`
- CI workflow: `.github/workflows/api-semver.yml`

## Distributed Compose Smoke Test
Use the v1 coordinator + 2 worker topology:

```bash
docker compose -f docker/compose/ffq.yml up --build -d
docker compose -f docker/compose/ffq.yml ps
docker compose -f docker/compose/ffq.yml logs -f coordinator worker-1 worker-2
```

Cleanup:

```bash
docker compose -f docker/compose/ffq.yml down -v
```

Run distributed integration runner against the compose endpoint:

```bash
FFQ_COORDINATOR_ENDPOINT=http://127.0.0.1:50051 ./scripts/run-distributed-integration.sh
```

Notes:
- script waits for coordinator + both worker shuffle endpoints before running tests.
- script uses deterministic temp path `target/tmp/integration_distributed` and cleans it automatically
  (set `FFQ_KEEP_INTEGRATION_TMP=1` to keep artifacts).

One-command 13.2 commands:

```bash
make test-13.2-embedded
make test-13.2-distributed
make test-13.2-parity
```

## License of contributions
By submitting a contribution, you agree that it will be licensed under the project’s license (Apache License 2.0), unless you explicitly state otherwise.

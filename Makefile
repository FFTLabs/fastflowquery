.PHONY: \
	clean \
	build \
	build-vector \
	run \
	plan \
	tree \
	test-planner \
	test-unit \
	test \
	test-13.1-core \
	test-13.1-vector \
	test-13.1-distributed \
	test-13.1 \
	bless-13.1-snapshots \
	test-13.2-embedded \
	test-13.2-distributed \
	test-13.2-parity

clean:
	cargo clean

build:
	cargo build

build-vector:
	cargo build -p ffq-client --features vector

run:
	cargo run -p ffq-client -- "SELECT id FROM t WHERE id = 1 LIMIT 10"

plan:
	cargo run -p ffq-client -- --plan "SELECT id FROM t WHERE id = 1 LIMIT 10"

tree:
	cargo tree -p ffq-client

test-planner:
	cargo test -p ffq-planner

test-unit:
	cargo test --workspace --lib

test:
	cargo test

# -----------------------------
# 13.1 Correctness suite
# -----------------------------

test-13.1-core:
	cargo test -p ffq-planner --test optimizer_golden
	cargo test -p ffq-client --test embedded_hash_join
	cargo test -p ffq-client --test embedded_hash_aggregate

test-13.1-vector:
	cargo test -p ffq-planner --test optimizer_golden --features vector
	cargo test -p ffq-execution --features vector
	cargo test -p ffq-client --features vector --lib
	cargo test -p ffq-client --features vector --test embedded_vector_topk

test-13.1-distributed:
	cargo test -p ffq-client --test distributed_runtime_roundtrip --features distributed

test-13.1: test-13.1-core test-13.1-vector test-13.1-distributed

bless-13.1-snapshots:
	BLESS=1 cargo test -p ffq-planner --test optimizer_golden
	BLESS=1 cargo test -p ffq-planner --test optimizer_golden --features vector

test-13.2-distributed:
	./scripts/run-distributed-integration.sh

test-13.2-embedded:
	cargo test -p ffq-client --test integration_parquet_fixtures
	cargo test -p ffq-client --test integration_embedded

test-13.2-parity:
	@set -euo pipefail; \
	docker compose -f docker/compose/ffq.yml up --build -d; \
	trap 'docker compose -f docker/compose/ffq.yml down -v' EXIT; \
	$(MAKE) test-13.2-embedded; \
	$(MAKE) test-13.2-distributed

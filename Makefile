SHELL := /bin/bash

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
	test-fast \
	test-slow-official \
	test-13.1-core \
	test-13.1-vector \
	test-13.1-distributed \
	test-13.1 \
	bless-13.1-snapshots \
	test-13.2-embedded \
	test-13.2-distributed \
	test-13.2-parity \
	bench-13.3-embedded \
	bench-13.3-distributed \
	bench-13.3-rag \
	bench-v2-window-embedded \
	bench-v2-window-distributed \
	bench-v2-window-compare \
	bench-v2-adaptive-shuffle-embedded \
	bench-v2-adaptive-shuffle-distributed \
	bench-v2-adaptive-shuffle-compare \
	bench-v2-join-radix \
	bench-13.4-official-embedded \
	bench-13.4-official-distributed \
	bench-13.4-official \
	bench-13.3-compare \
	tpch-dbgen-build \
	tpch-dbgen-sf1 \
	tpch-dbgen-parquet \
	validate-tpch-dbgen-manifests \
	compare-13.3 \
	repl \
	repl-smoke \
	ffi-build \
	ffi-example \
	python-wheel \
	python-dev-install \
	docs-v2-guardrails

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

test-fast: test

test-slow-official:
	cargo test -p ffq-client --test tpch_catalog_profiles -- --ignored --nocapture

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

bench-13.3-embedded:
	./scripts/run-bench-13.3.sh

bench-13.3-distributed:
	FFQ_BENCH_MODE=distributed ./scripts/run-bench-13.3.sh

bench-13.3-rag:
	FFQ_BENCH_MODE=embedded FFQ_BENCH_RAG_MATRIX="$${FFQ_BENCH_RAG_MATRIX:-1000,16,10,1.0;5000,32,10,0.8;10000,64,10,0.2}" ./scripts/run-bench-13.3.sh

bench-v2-window-embedded:
	FFQ_BENCH_MODE=embedded FFQ_BENCH_INCLUDE_WINDOW=1 FFQ_BENCH_INCLUDE_RAG=0 FFQ_BENCH_WINDOW_MATRIX="$${FFQ_BENCH_WINDOW_MATRIX:-narrow;wide;skewed;many_exprs}" ./scripts/run-bench-v2-window.sh

bench-v2-window-distributed:
	FFQ_BENCH_MODE=distributed FFQ_BENCH_INCLUDE_WINDOW=1 FFQ_BENCH_INCLUDE_RAG=0 FFQ_BENCH_WINDOW_MATRIX="$${FFQ_BENCH_WINDOW_MATRIX:-narrow;wide;skewed;many_exprs}" ./scripts/run-bench-v2-window.sh

bench-v2-window-compare:
	@test -n "$$BASELINE" || (echo "BASELINE is required (json file or dir)" && exit 1)
	@test -n "$$CANDIDATE" || (echo "CANDIDATE is required (json file or dir)" && exit 1)
	./scripts/compare-bench-13.3.py --baseline "$$BASELINE" --candidate "$$CANDIDATE" --threshold "$${THRESHOLD:-0.10}" --threshold-file "$${THRESHOLD_FILE:-tests/bench/thresholds/window_regression_thresholds.json}"

bench-v2-adaptive-shuffle-embedded:
	FFQ_BENCH_MODE=embedded FFQ_BENCH_INCLUDE_WINDOW=0 FFQ_BENCH_INCLUDE_RAG=0 FFQ_BENCH_INCLUDE_ADAPTIVE_SHUFFLE=1 FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX="$${FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX:-tiny;large;skewed;mixed}" ./scripts/run-bench-v2-adaptive-shuffle.sh

bench-v2-adaptive-shuffle-distributed:
	FFQ_BENCH_MODE=distributed FFQ_BENCH_INCLUDE_WINDOW=0 FFQ_BENCH_INCLUDE_RAG=0 FFQ_BENCH_INCLUDE_ADAPTIVE_SHUFFLE=1 FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX="$${FFQ_BENCH_ADAPTIVE_SHUFFLE_MATRIX:-tiny;large;skewed;mixed}" ./scripts/run-bench-v2-adaptive-shuffle.sh

bench-v2-adaptive-shuffle-compare:
	@test -n "$$BASELINE" || (echo "BASELINE is required (json file or dir)" && exit 1)
	@test -n "$$CANDIDATE" || (echo "CANDIDATE is required (json file or dir)" && exit 1)
	./scripts/compare-bench-13.3.py --baseline "$$BASELINE" --candidate "$$CANDIDATE" --threshold "$${THRESHOLD:-0.10}" --threshold-file "$${THRESHOLD_FILE:-tests/bench/thresholds/adaptive_shuffle_regression_thresholds.json}"

bench-v2-join-radix:
	cargo run -p ffq-client --example bench_join_radix

bench-13.4-official-embedded:
	FFQ_BENCH_MODE=embedded FFQ_BENCH_TPCH_SUBDIR="$${FFQ_BENCH_TPCH_SUBDIR:-tpch_dbgen_sf1_parquet}" ./scripts/run-bench-13.4-tpch-official.sh

bench-13.4-official-distributed:
	FFQ_BENCH_MODE=distributed FFQ_BENCH_TPCH_SUBDIR="$${FFQ_BENCH_TPCH_SUBDIR:-tpch_dbgen_sf1_parquet}" ./scripts/run-bench-13.4-tpch-official.sh

bench-13.4-official: bench-13.4-official-embedded

bench-13.3-compare:
	@test -n "$$BASELINE" || (echo "BASELINE is required (json file or dir)" && exit 1)
	@test -n "$$CANDIDATE" || (echo "CANDIDATE is required (json file or dir)" && exit 1)
	@if [ -n "$$THRESHOLD_FILE" ]; then \
		./scripts/compare-bench-13.3.py --baseline "$$BASELINE" --candidate "$$CANDIDATE" --threshold "$${THRESHOLD:-0.10}" --threshold-file "$$THRESHOLD_FILE"; \
	else \
		./scripts/compare-bench-13.3.py --baseline "$$BASELINE" --candidate "$$CANDIDATE" --threshold "$${THRESHOLD:-0.10}"; \
	fi

tpch-dbgen-build:
	./scripts/build-tpch-dbgen.sh

tpch-dbgen-sf1:
	TPCH_SCALE=1 ./scripts/generate-tpch-dbgen-sf1.sh

tpch-dbgen-parquet:
	./scripts/convert-tpch-dbgen-parquet.sh

validate-tpch-dbgen-manifests:
	./scripts/validate-tpch-dbgen-manifests.py

compare-13.3:
	$(MAKE) bench-13.3-compare BASELINE="$$BASELINE" CANDIDATE="$$CANDIDATE" THRESHOLD="$${THRESHOLD:-0.10}"

repl:
	@if [ -n "$$CATALOG" ]; then \
		cargo run -p ffq-client -- repl --catalog "$$CATALOG"; \
	else \
		cargo run -p ffq-client -- repl; \
	fi

repl-smoke:
	./scripts/run-repl-smoke.sh

ffi-build:
	cargo build -p ffq-client --features ffi

ffi-example:
	./scripts/run-ffi-c-example.sh "$${PARQUET_PATH:-tests/fixtures/parquet/lineitem.parquet}"

python-wheel:
	python -m pip install --upgrade maturin
	maturin build --release

python-dev-install:
	python -m pip install --upgrade maturin
	maturin develop --features python

docs-v2-guardrails:
	python3 scripts/validate-docs-v2.py

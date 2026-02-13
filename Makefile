.PHONY: build

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

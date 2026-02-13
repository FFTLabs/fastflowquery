.PHONY: build

clean:
	cargo clean

build:
	cargo build

build-vector:
	cargo build -p ffq-client --features vector

run:
	cargo run -p ffq-client -- "SELECT a FROM t LIMIT 5"

tree:
	cargo tree -p ffq-client

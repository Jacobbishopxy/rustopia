ifneq (,$(wildcard ./.env))
    include .env
    export
endif

deps:
	cd ./ua-application && cargo fetch

dev:
	cargo run

dev-offline:
	cargo run offline

build:
	cargo build --release

# prod:

.PHONY: deps dev dev-offline build

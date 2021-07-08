ifneq (,$(wildcard ./.env))
    include .env
    export
endif

postgres:
	docker run --name postgres12 -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=secret -d postgres:12-alpine

createdb:
	docker exec -it postgres12 createdb --username=root --owner=root ubiquitous_alchemy

pg-uuid:
	docker exec -it postgres12 psql -U root -d 'ubiquitous_alchemy' -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'

dropdb:
	docker exec -it postgres12 dropdb ubiquitous_alchemy

deps:
	cd ./ua-application && cargo fetch

dev:
	cargo run

dev-offline:
	cargo run offline

build:
	cargo build --release

# prod:

.PHONY: postgres createdb pg-uuid dropdb deps dev dev-offline build

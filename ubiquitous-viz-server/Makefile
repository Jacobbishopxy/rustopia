ifneq (,$(wildcard ./.env))
    include .env
    export
endif

deps:
	cd ./uv-backend && cargo fetch && cd ../uv-frontend && yarn

dev-backend:
	cargo run ./uv-backend

dev-frontend:
	yarn --cwd ./uv-frontend start

build-backend:
	cd ./uv-backend && cargo build --release

build-frontend:
	cd ./uv-frontend && yarn build

# dev:

dev-prod:
	cargo run ./uv-backend prod

# prod:

.PHONY: deps dev-backend dev-frontend build-backend build-frontend dev-prod

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

dev-backend:
	cargo run ./uv-backend

dev-frontend:
	yarn --cwd ./uv-frontend start

.PHONY: dev-backend dev-frontend

# Ubiquitous Viz Server

Ubiquitous viz server, as a web server supporting user interaction with backend, must work with ubiquitous alchemy server, who gives API supports.

## Requirements

`cp .env.template .env` and replace with your own configs.

## Dependencies

1. actix-cors
1. actix-files
1. actix-web
1. serde

## Project structure

1. [uv-backend](./uv-backend/src/lib.rs)

1. [uv-frontend](./uv-frontend/package.json)

# Pingoose

Please create a `.env` file first (see `.env.template`).

## Executable

- [mock_receiver](./src/mock_receiver.rs): receiving mock message.
- [server](./src/server.rs): server side gRPC responder.
- [client](./src/client.rs): client side gRPC pinger.

## Development

- `make mock-recv`: start a mock receiver for accepting disconnect info from client side
- `make server`: start a server side responder
- `make client`: start a client who keeps pinging the server

## Production

With `cargo build --release`, you can get three binary executables under `target/release` directory, which are `server`, `client` and `mock-receiver` (just ignore it, since we don't need it in production, replace it by you own report receiver).

Then we can then `./server` to start our responder, and on the client side we can do `./client` to start our pinger.

Happy coding & have fun!

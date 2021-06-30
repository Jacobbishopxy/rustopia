use tonic::transport::Server;

// A gRPC responder
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = dotenv::var("SERVER_ADDR")
        .expect("Expected SERVER_ADDR to be set in env!")
        .parse()?;
    let pin_goose = pingoose::PinGooser::default();

    Server::builder()
        .add_service(pingoose::PinGooseServer::new(pin_goose))
        .serve(addr)
        .await?;

    Ok(())
}

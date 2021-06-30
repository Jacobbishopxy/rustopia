use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let pin_goose = ping_pong::PinGoose::default();

    Server::builder()
        .add_service(ping_pong::PingerServer::new(pin_goose))
        .serve(addr)
        .await?;

    Ok(())
}

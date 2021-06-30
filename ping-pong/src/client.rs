use core::time;
use std::thread;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ping_pong::PingerClient::connect("http://[::1]:50051").await?;

    loop {
        thread::sleep(time::Duration::from_secs(5));
        let resp = client.ping(()).await?;
        println!("Response: {:?}", resp);
    }
}

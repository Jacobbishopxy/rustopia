use core::time;
use std::thread;

use hyper::{Body, Client, Method, Request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    // Server address
    let listening_to =
        dotenv::var("LISTENING_TO").expect("Expected LISTENING_TO to be set in env!");
    // Reporter address
    let reporting_to =
        dotenv::var("REPORTING_TO").expect("Expected REPORTING_TO to be set in env!");
    // Message send to reporter, if disconnected
    let msg =
        dotenv::var("MESSAGE").unwrap_or_else(|_| format!("Failed Pinging {:?}", &listening_to));
    let heart_beat = dotenv::var("HEART_BEAT")
        .expect("Expected HEART_BEAT to be set in env!")
        .parse()
        .unwrap();

    // gRPC client
    let mut client = ping_pong::PingerClient::connect(listening_to.clone()).await?;

    loop {
        match client.ping(()).await {
            Ok(resp) => {
                println!("Response: {:?}", resp);
            }
            Err(_) => {
                Reporter::new(reporting_to).send_report(msg).await?;
                panic!("Ping failed!");
            }
        }

        thread::sleep(time::Duration::from_secs(heart_beat));
    }
}

struct Reporter {
    addr: String,
}

impl Reporter {
    fn new(addr: String) -> Self {
        Reporter { addr }
    }

    async fn send_report(
        &self,
        msg: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(">>> sending report...");

        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.addr)
            .header("content-type", "application/json")
            .body(Body::from(msg))?;

        let client = Client::new();
        client.request(req).await?;

        println!(">>> report sended!");

        Ok(())
    }
}

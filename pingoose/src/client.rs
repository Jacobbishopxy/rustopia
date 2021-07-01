use core::time;
use std::thread;

use hyper::{Body, Client, Method, Request};
use hyper_tls::HttpsConnector;

type ErrorType = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), ErrorType> {
    // load config from .env file
    let config = Config::new();

    // start scheduling pinging job
    ignite(config).await
}

async fn ignite(config: Config) -> Result<(), ErrorType> {
    // gRPC client
    let mut client = pingoose::PinGooseClient::connect(config.listening_to.clone()).await?;
    let reporter = Reporter::new(config.reporting_to.clone(), config.tls);

    loop {
        if config.arg_config.test {
            // if in test case, server side would respond us with it's local time
            match client.ping(()).await {
                Ok(resp) => {
                    println!("Response: {:?}", resp);
                }
                Err(_) => {
                    reporter.send_report(config.message.clone()).await?;
                    panic!("Ping failed!");
                }
            }
        } else {
            // if in normal case, server side would send nothing to us
            match client.ping_simple(()).await {
                Ok(_) => {
                    // println!(
                    //     "Response: {:?}",
                    //     chrono::Utc::now().format("%b %-d, %-I:%M").to_string()
                    // );
                }
                Err(_) => {
                    reporter.send_report(config.message.clone()).await?;
                    panic!("Ping failed!");
                }
            }
        }

        thread::sleep(time::Duration::from_secs(config.heart_beat.clone()));
    }
}

struct Config {
    // Server address
    listening_to: String,
    // Reporter address
    reporting_to: String,
    // Message send to reporter, if disconnected
    message: String,
    // Client pinging frequency
    heart_beat: u64,
    // true if reporter a https, default false
    tls: bool,

    // arg config
    arg_config: ArgConfig,
}

impl Config {
    fn new() -> Self {
        dotenv::dotenv().ok();

        let listening_to =
            dotenv::var("LISTENING_TO").expect("Expected LISTENING_TO to be set in env!");

        let reporting_to =
            dotenv::var("REPORTING_TO").expect("Expected REPORTING_TO to be set in env!");

        let message = dotenv::var("MESSAGE")
            .unwrap_or_else(|_| format!("Failed Pinging {:?}", &listening_to));

        let heart_beat = dotenv::var("HEART_BEAT")
            .expect("Expected HEART_BEAT to be set in env!")
            .parse()
            .expect("Failed to parse HEART_BEAT in env!");

        let tls: bool = dotenv::var("TLS")
            .expect("Expected TLS to be set in env!")
            .parse()
            .unwrap_or_else(|_| false);

        let arg_config = ArgConfig::new(std::env::args());

        Config {
            listening_to,
            reporting_to,
            message,
            heart_beat,
            tls,
            arg_config,
        }
    }
}

struct ArgConfig {
    test: bool,
}

impl ArgConfig {
    fn new(mut args: std::env::Args) -> ArgConfig {
        args.next();

        let test = match args.next() {
            Some(s) if s == "test" => true,
            _ => false,
        };

        ArgConfig { test }
    }
}

struct Reporter {
    addr: String,
    tls: bool,
}

impl Reporter {
    fn new(addr: String, tls: bool) -> Self {
        Reporter { addr, tls }
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

        let resp;

        if self.tls {
            let https = HttpsConnector::new();
            let client = Client::builder().build::<_, hyper::Body>(https);
            resp = client.request(req).await?;
        } else {
            let client = Client::new();
            resp = client.request(req).await?;
        }

        let body = hyper::body::to_bytes(resp).await?;
        let body = String::from_utf8(body.to_vec()).unwrap();

        println!("response body: {:?}", body);

        println!(">>> report sended!");

        Ok(())
    }
}

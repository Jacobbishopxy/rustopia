use std::convert::Infallible;
use std::net::SocketAddr;

use hyper::body::Buf;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Request, Response, Server, StatusCode};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;

async fn mock_post_response(req: Request<Body>) -> Result<Response<Body>> {
    let whole_body = hyper::body::aggregate(req).await?;

    let data: serde_json::Value = serde_json::from_reader(whole_body.reader())?;
    let resp = serde_json::to_string(&data)?;

    println!(">>> {:?}", resp);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("ok"))?;
    Ok(response)
}

// A mock report receiver
#[tokio::main]
async fn main() {
    let reporting_to =
        dotenv::var("REPORTING_TO").expect("Expected REPORTING_TO to be set in env!");
    let addr: SocketAddr = reporting_to
        .replace("http://", "")
        .parse()
        .expect("Unable to parse socket address");

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(mock_post_response)) });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

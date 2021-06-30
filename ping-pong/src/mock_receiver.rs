use std::convert::Infallible;
use std::net::SocketAddr;

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Request, Response, Server, StatusCode};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;

async fn mock_post_response(_: Request<Body>) -> Result<Response<Body>> {
    println!("received a report");
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("ok"))?;
    Ok(response)
}

// A mock report receiver
#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8088));

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(mock_post_response)) });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

//! Ubiquitous Viz Server
//!
//! More details please check `lib.rs`.

use actix_cors::Cors;
use actix_web::{client::Client, middleware::Logger, web, App, HttpServer};

use uv_backend::{constant, frontend, proxy_agent, util};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let cfg = constant::Config::new();

    log::info!(
        "Rust Actix Server running... http://{}:{}",
        cfg.service_host,
        cfg.service_port
    );

    match util::is_prod() {
        true => prod(cfg).await,
        false => dev(cfg).await,
    }
}

/// development mode: serving proxy for a local frontend server
async fn dev(cfg: constant::Config) -> std::io::Result<()> {
    let forward_url = format!("http://{}:{}", cfg.forward_host, cfg.forward_port);
    let forward_url = util::str_to_url(&forward_url).expect("Cannot parser forward URL");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .supports_credentials()
            .max_age(3600);

        App::new()
            .wrap(Logger::default())
            .wrap(cors) // enable CORS support
            .data(Client::new()) // proxy incoming request to external service
            .data(forward_url.clone())
            .default_service(web::route().to(proxy_agent::forward))
    })
    .bind(format!("{}:{}", cfg.service_host, cfg.service_port))?
    .run()
    .await
}

/// production mode: serving frontend as static file
async fn prod(cfg: constant::Config) -> std::io::Result<()> {
    let forward_url = format!("http://{}:{}", cfg.forward_host, cfg.forward_port);
    let forward_url = util::str_to_url(&forward_url).expect("Cannot parser forward URL");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .service(frontend::index()) // serving static HTML file built by React
            .data(Client::new()) // proxy incoming request to external service
            .data(forward_url.clone())
            .default_service(web::route().to(proxy_agent::forward))
    })
    .bind(format!("{}:{}", cfg.service_host, cfg.service_port))?
    .run()
    .await
}

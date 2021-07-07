use actix_cors::Cors;
use actix_web::{middleware::Logger, App, HttpServer};

use uv_backend::{constant::CFG, frontend};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let (_uri, host, port) = (
        CFG.get("URI").unwrap(),
        CFG.get("SERVICE_HOST").unwrap(),
        CFG.get("SERVICE_PORT").unwrap(),
    );

    log::info!("Rust Actix Server running... http://{}:{}", host, port);
    HttpServer::new(|| {
        App::new()
            // TODO: dev mode use cors
            .wrap(
                Cors::default()
                    .allowed_origin("http://localhost:3000")
                    .allow_any_method()
                    .allow_any_header()
                    .supports_credentials()
                    .max_age(3600),
            )
            .wrap(Logger::default())
            // TODO: prod mode use static file
            .service(frontend::index())
    })
    .bind(format!("{}:{}", host, port))?
    .run()
    .await
}

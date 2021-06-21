use std::sync::Mutex;

use actix_web::{middleware, web, App, HttpServer};
use log::info;

use dyn_conn::controllers::{scope_api, scope_util};
use dyn_conn::models::DynPoolOptions;
use dyn_conn::ConnStore;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let (host, port) = ("127.0.0.1", 8080);

    let dyn_conn = ConnStore::<DynPoolOptions>::new();
    let dyn_conn_data = web::Data::new(Mutex::new(dyn_conn));

    info!("Rust Actix Server running... http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new()
            .app_data(dyn_conn_data.clone())
            .wrap(middleware::Logger::default())
            .service(scope_util("/util"))
            .service(scope_api("/api"))
    })
    .bind(format!("{}:{}", host, port))?
    .run()
    .await
}

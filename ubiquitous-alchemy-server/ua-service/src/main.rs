use std::sync::Mutex;

use actix_web::{web, App, HttpServer};
use log::info;

use dyn_conn::DynConn;
use ua_service::constant::CFG;
use ua_service::controller::{dynamic, query, schema};
use ua_service::service::ServiceDynConn;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let (_uri, host, port) = (
        CFG.get("URI").unwrap(),
        CFG.get("SERVICE_HOST").unwrap(),
        CFG.get("SERVICE_PORT").unwrap(),
    );

    // TODO: persistence and it's initialization

    let mutex_service_dyn_conn = Mutex::new(ServiceDynConn::new(DynConn::new()));
    let mutex_service_dyn_conn = web::Data::new(mutex_service_dyn_conn);

    info!("Rust Actix Server running... http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new().app_data(mutex_service_dyn_conn.clone()).service(
            web::scope("/api")
                .service(dynamic::scope("/dyn"))
                .service(query::scope("/query"))
                .service(schema::scope("/schema")),
        )
    })
    .bind(format!("{}:{}", host, port))?
    .run()
    .await
}

use std::sync::Mutex;

use actix_web::{web, App, HttpServer};
use log::info;

use ua_application::constant::CFG;
use ua_application::controller::{dynamic, query, schema};
use ua_application::model::{UaPersistence, UaStore};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let (uri, host, port) = (
        CFG.get("URI").unwrap(),
        CFG.get("SERVICE_HOST").unwrap(),
        CFG.get("SERVICE_PORT").unwrap(),
    );

    let mut ua_store = UaStore::new();
    let ua_persistence = UaPersistence::new(uri).await;
    ua_store
        .attach_persistence(Box::new(ua_persistence))
        .await
        .expect("Attach store failed!");
    let mutex_service_dyn_conn = Mutex::new(ua_store);
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

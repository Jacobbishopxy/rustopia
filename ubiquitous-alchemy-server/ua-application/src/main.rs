use std::sync::Mutex;

use actix_web::{web, App, HttpServer};

use ua_application::constant::CFG;
use ua_application::controller::{self, configuration, query, schema};
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

    let ua_store = match is_offline() {
        true => UaStore::new(),
        false => {
            let mut us = UaStore::new();
            let ua_persistence = UaPersistence::new(uri)
                .await
                .expect("Persistence database connection error");
            ua_persistence
                .init_table()
                .await
                .expect("Init table failed");
            us.attach_persistence(Box::new(ua_persistence))
                .await
                .expect("Attach store failed!");
            us
        }
    };
    let mutex_service_dyn_conn = Mutex::new(ua_store);
    let mutex_service_dyn_conn = web::Data::new(mutex_service_dyn_conn);

    log::info!("Rust Actix Server running... http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new().app_data(mutex_service_dyn_conn.clone()).service(
            web::scope("/api")
                .service(controller::index)
                .service(configuration::scope("/cfg"))
                .service(query::scope("/query"))
                .service(schema::scope("/schema")),
        )
    })
    .bind(format!("{}:{}", host, port))?
    .run()
    .await
}

fn is_offline() -> bool {
    let mut args = std::env::args();

    args.next();

    match args.next() {
        Some(m) if m == "offline" => true,
        _ => false,
    }
}

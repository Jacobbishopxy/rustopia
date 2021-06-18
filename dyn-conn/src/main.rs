use std::sync::Mutex;

use actix_web::{middleware, web, App, HttpServer};

use dyn_conn::handlers;
use dyn_conn::DynConn;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let dyn_conn = DynConn::new();
    let dyn_conn_data = web::Data::new(Mutex::new(dyn_conn));

    HttpServer::new(move || {
        App::new()
            .app_data(dyn_conn_data.clone())
            .wrap(middleware::Logger::default())
            .service(web::scope("/util").service(handlers::check_connection))
            .service(
                web::scope("/api")
                    .service(handlers::index)
                    .service(handlers::info)
                    .service(handlers::info_new),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

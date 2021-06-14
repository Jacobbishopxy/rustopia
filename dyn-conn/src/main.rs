use std::sync::Mutex;

use actix_web::{middleware, web, App, HttpResponse, HttpServer};

use dyn_conn::models::DynConn;

async fn index(dyn_conn: web::Data<Mutex<DynConn>>) -> HttpResponse {
    let keys = dyn_conn.as_ref().lock().unwrap().show_keys();
    let body = serde_json::json!(keys);

    HttpResponse::Ok().body(body)
}

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
            .service(web::resource("/").to(index))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

use actix_web::{web, App, HttpServer};
use log::info;

use ua_dao::dao::DaoPG;
use ua_service::constant::CFG;
use ua_service::endpoint::schema;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let uri = CFG.get("URI").unwrap();
    let dao = DaoPG::new(uri, 10).await;

    info!("Rust Actix Server running... http://localhost:8080");
    HttpServer::new(move || {
        App::new().data(dao.clone()).service(
            web::scope("/api")
                .service(schema::index)
                .service(schema::table_create)
                .service(schema::table_alter)
                .service(schema::table_drop)
                .service(schema::table_rename)
                .service(schema::table_truncate),
        )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

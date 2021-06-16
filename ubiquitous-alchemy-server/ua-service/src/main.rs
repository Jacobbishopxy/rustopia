use actix_web::{web, App, HttpServer};
use log::info;

use ua_dao::dao::{DaoOptions, DaoPG};
use ua_service::constant::CFG;
use ua_service::endpoint::{query, schema};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let (uri, host, port) = (
        CFG.get("URI").unwrap(),
        CFG.get("SERVICE_HOST").unwrap(),
        CFG.get("SERVICE_PORT").unwrap(),
    );
    let dao = DaoOptions::PG(DaoPG::new(uri, 10).await);

    info!("Rust Actix Server running... http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new().data(dao.clone()).service(
            web::scope("/api")
                .service(schema::index)
                .service(schema::table_list)
                .service(schema::table_create)
                .service(schema::table_alter)
                .service(schema::table_drop)
                .service(schema::table_rename)
                .service(schema::table_truncate)
                .service(schema::index_create)
                .service(schema::index_drop)
                .service(schema::foreign_key_create)
                .service(schema::foreign_key_drop)
                .service(query::table_select),
        )
    })
    .bind(format!("{}:{}", host, port))?
    .run()
    .await
}

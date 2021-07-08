use actix_files::Files;

/// serving frontend static file (built as prod), only work in prod mode.
pub fn index() -> Files {
    let resp = Files::new("/", "./uv-frontend/build/").index_file("index.html");

    resp
}

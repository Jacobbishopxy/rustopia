use actix_files::Files;

pub fn index() -> Files {
    let resp = Files::new("/", "./uv-frontend/build/").index_file("index.html");

    resp
}

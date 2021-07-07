use url::Url;

pub fn is_prod() -> bool {
    let mut args = std::env::args();

    args.next();
    args.next();

    match args.next() {
        Some(m) if m == "prod" => true,
        _ => false,
    }
}

pub fn str_to_url(s: &str) -> Url {
    Url::parse(s).unwrap()
}

use url::{ParseError, Url};

pub fn is_prod() -> bool {
    let mut args = std::env::args();

    args.next();
    args.next();

    match args.next() {
        Some(m) if m == "prod" => true,
        _ => false,
    }
}

pub fn str_to_url(s: &str) -> Result<Url, ParseError> {
    Url::parse(s)
}

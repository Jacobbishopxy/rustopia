use std::collections::HashMap;

lazy_static::lazy_static! {
    pub static ref CFG: HashMap<&'static str, String> = {

        dotenv::dotenv().ok();

        let mut map = HashMap::new();

        map.insert("URI", dotenv::var("URI").expect(&exception_msg("URI")));
        map.insert("SERVICE_HOST", dotenv::var("SERVICE_HOST").expect(&exception_msg("SERVICE_HOST")));
        map.insert("SERVICE_PORT", dotenv::var("SERVICE_PORT").expect(&exception_msg("SERVICE_PORT")));
        map.insert("FORWARD_HOST", dotenv::var("FORWARD_HOST").expect(&exception_msg("FORWARD_HOST")));
        map.insert("FORWARD_PORT", dotenv::var("FORWARD_PORT").expect(&exception_msg("FORWARD_PORT")));

        map
    };
}

fn exception_msg(arg: &str) -> String {
    format!("Expected {} to be set in env!", arg)
}

pub struct Config {
    pub uri: String,
    pub service_host: String,
    pub service_port: String,
    pub forward_host: String,
    pub forward_port: String,
}

impl Config {
    pub fn new() -> Self {
        let (uri, service_host, service_port, forward_host, forward_port) = (
            CFG.get("URI").unwrap(),
            CFG.get("SERVICE_HOST").unwrap(),
            CFG.get("SERVICE_PORT").unwrap(),
            CFG.get("FORWARD_HOST").unwrap(),
            CFG.get("FORWARD_PORT").unwrap(),
        );

        Config {
            uri: uri.to_owned(),
            service_host: service_host.to_owned(),
            service_port: service_port.to_owned(),
            forward_host: forward_host.to_owned(),
            forward_port: forward_port.to_owned(),
        }
    }
}

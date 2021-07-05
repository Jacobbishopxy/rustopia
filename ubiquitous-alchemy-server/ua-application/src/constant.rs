use std::collections::HashMap;

lazy_static::lazy_static! {
    pub static ref CFG: HashMap<&'static str, String> = {

        dotenv::dotenv().ok();

        let mut map = HashMap::new();

        map.insert(
            "URI", dotenv::var("URI").expect("Expected URI to be set in env!"),
        );
        map.insert(
            "SERVICE_HOST", dotenv::var("SERVICE_HOST").expect("Expected SERVICE_HOST to be set in env!"),
        );
        map.insert(
            "SERVICE_PORT", dotenv::var("SERVICE_PORT").expect("Expected SERVICE_PORT to be set in env!"),
        );

        map
    };
}

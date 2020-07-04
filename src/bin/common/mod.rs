extern crate sha2;

use self::sha2::{Sha256, Digest};
use std::collections::HashMap;

pub fn hash(input: &[u8]) -> Vec<u8> {
            let mut hasher = Sha256::new();
            hasher.input(input);
            let key = hasher.result();

            key.to_vec()
}

pub fn get_config() -> std::io::Result<HashMap<String, String>> {
    let mut config = config::Config::default();
    config
        .merge(config::File::with_name("config")).unwrap()
        .merge(config::Environment::with_prefix("KADEMLIA")).unwrap();

    let result = config.try_into::<HashMap<String, String>>();
    match result {
        Ok(configs_map) => {
            Ok(configs_map)
        }
        Err(err) => {
            Err(std::io::Error::new(std::io::ErrorKind::Other, err))
        }
    }
}

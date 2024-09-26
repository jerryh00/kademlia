extern crate config;
extern crate serde;
extern crate sha2;

use self::config::{Config, File, FileFormat};
use self::serde::Deserialize;
use self::sha2::{Digest, Sha256};

pub fn hash(input: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let key = hasher.finalize();

    key.to_vec()
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server_address: String,
    #[serde(default)]
    pub server_port: u16,
    #[serde(default)]
    pub sleep_time: u64,
    #[serde(default)]
    pub test_store_interval: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            server_address: "127.0.0.1".to_string(),
            server_port: 17000,
            sleep_time: 1000,
            test_store_interval: 1000,
        }
    }
}

pub fn get_config() -> std::io::Result<AppConfig> {
    let builder = Config::builder()
        .add_source(File::new("config.yaml", FileFormat::Yaml).required(false))
        .add_source(config::Environment::with_prefix("KADEMLIA"));

    let result = builder.build();

    match result {
        Ok(configs) => Ok(configs.try_deserialize().unwrap()),
        Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err)),
    }
}

extern crate sha2;

use std::time;
use self::sha2::{Sha256, Digest};

pub const TEST_STORE_INTERVAL:time::Duration = time::Duration::from_millis(1000);

pub fn hash(input: &[u8]) -> Vec<u8> {
            let mut hasher = Sha256::new();
            hasher.input(input);
            let key = hasher.result();

            key.to_vec()
}

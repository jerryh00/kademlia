extern crate clap;
extern crate config;
extern crate env_logger;
extern crate kademlia;
extern crate log;
extern crate rand;

use clap::{Arg, Command};
use env_logger::Env;
use log::{debug, error, info, warn};
use rand::Rng;
use std::{thread, time};

mod common;

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let matches = Command::new("node")
        .arg(
            Arg::new("test")
                .short('t')
                .long("test")
                .num_args(0)
                .help("run in test mode"),
        )
        .get_matches();
    info!("Init node");
    let configs = common::get_config()?;
    info!("configs={:?}", configs);

    let test = matches.get_flag("test");
    info!("test={}", test);
    let result = kademlia::Node::new(
        false,
        configs.server_address.as_bytes(),
        configs.server_port,
        test,
    );
    match result {
        Ok(node_user) => {
            debug!("key_len={:?}", node_user.key_len);
            let key: Vec<u8> = rand::thread_rng()
                .sample_iter(&rand::distributions::Standard)
                .take(node_user.key_len)
                .collect();

            let sleep_time = time::Duration::from_millis(configs.sleep_time);
            while let Err(result) = node_user.store(&key, b"value0") {
                debug!("store result: {:?}", result);
                thread::sleep(sleep_time);
            }
            info!("node store ok");

            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());
                while let Err(result) = node_user.find_value(&key) {
                    warn!("find_value failed: i={}, {:?}", i, result);
                    let test_store_interval =
                        time::Duration::from_millis(configs.test_store_interval);
                    thread::sleep(2 * test_store_interval);
                }
                info!("find_value succeeded: i={}", i);
            }
        }
        Err(err) => {
            error!("node kademlia::Node::new failed: {:?}", err);
        }
    }

    loop {
        thread::park();
        info!("main() unparked");
    }
}

extern crate kademlia;
extern crate rand;
extern crate env_logger;
extern crate log;
extern crate clap;
extern crate config;

use rand::Rng;
use std::{thread, time};
use log::{warn, info, debug, error};
use clap::{Arg, App};
use env_logger::Env;

mod common;

fn main() -> std::io::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();

    let matches = App::new("node")
        .arg(Arg::with_name("test")
             .short("t")
             .long("test")
             .help("run in test mode")
             .takes_value(false))
        .get_matches();
    info!("Init node");
    let configs_map = common::get_config()?;
    info!("configs_map={:?}", configs_map);

    let test = matches.is_present("test");
    info!("test={}", test);
    let result = kademlia::Node::new(false, configs_map["server_address"].as_bytes(), configs_map["server_port"].parse::<u16>().unwrap(), test);
    match result {
        Ok(node_user) => {
            debug!("key_len={:?}", node_user.key_len);
            let key:Vec<u8> = rand::thread_rng().sample_iter(&rand::distributions::Standard).take(node_user.key_len).collect();

            let sleep_time = time::Duration::from_millis(configs_map["sleep_time"].parse::<u64>().unwrap());
            while let Err(result) = node_user.store(&key, b"value0") {
                debug!("store result: {:?}", result);
                thread::sleep(sleep_time);
            }
            info!("node store ok");

            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());
                while let Err(result) = node_user.find_value(&key) {
                    warn!("find_value failed: i={}, {:?}", i, result);
                    let test_store_interval = time::Duration::from_millis(configs_map["test_store_interval"].parse::<u64>().unwrap());
                    thread::sleep(2*test_store_interval);
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

extern crate kademlia;
extern crate env_logger;
extern crate log;
extern crate clap;

use std::{thread, time};
use log::{info, warn, error};
use clap::{Arg, App};
use env_logger::Env;

mod common;

fn main() -> std::io::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();

    let matches = App::new("server")
        .arg(Arg::with_name("test")
             .short("t")
             .long("test")
             .help("run in test mode")
             .takes_value(false))
        .get_matches();

    let test = matches.is_present("test");
    info!("test={}", test);
    info!("Init server");
    let configs_map = common::get_config()?;
    info!("configs_map={:?}", configs_map);

    let result = kademlia::Node::new(true, configs_map["server_address"].as_bytes(), configs_map["server_port"].parse::<u16>().unwrap(), test);
    match result {
        Ok(node_user) => {
            let sleep_time = time::Duration::from_millis(configs_map["sleep_time"].parse::<u64>().unwrap());
            thread::sleep(sleep_time);
            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());

                while let Err(result) = node_user.store(&key, &i.to_le_bytes()) {
                    warn!("store result: {:?}", result);
                    thread::sleep(sleep_time);
                }
                info!("node store ok");

                let test_store_interval = time::Duration::from_millis(configs_map["test_store_interval"].parse::<u64>().unwrap());
                thread::sleep(test_store_interval);
            }
        }
        Err(err) => {
            error!("server kademlia::Node::new failed: {:?}", err);
        }
    }

    loop {
        thread::park();
        info!("main() unparked");
    }
}

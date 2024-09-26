extern crate clap;
extern crate env_logger;
extern crate kademlia;
extern crate log;

use clap::{Arg, Command};
use env_logger::Env;
use log::{error, info, warn};
use std::{thread, time};

mod common;

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let matches = Command::new("server")
        .arg(
            Arg::new("test")
                .short('t')
                .long("test")
                .num_args(0)
                .help("run in test mode"),
        )
        .get_matches();

    let test = matches.get_flag("test");
    info!("test={}", test);
    info!("Init server");
    let configs = common::get_config()?;
    info!("configs={:?}", configs);

    let result = kademlia::Node::new(
        true,
        configs.server_address.as_bytes(),
        configs.server_port,
        test,
    );
    match result {
        Ok(node_user) => {
            let sleep_time = time::Duration::from_millis(configs.sleep_time);
            thread::sleep(sleep_time);
            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());

                while let Err(result) = node_user.store(&key, &i.to_le_bytes()) {
                    warn!("store result: {:?}", result);
                    thread::sleep(sleep_time);
                }
                info!("node store ok");

                let test_store_interval = time::Duration::from_millis(configs.test_store_interval);
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

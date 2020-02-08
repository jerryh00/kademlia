extern crate kademlia;
extern crate env_logger;
extern crate log;
extern crate clap;

use std::{thread, time};
use log::{info, warn, error};
use clap::{Arg, App};

mod common;

fn main() {
    env_logger::init();

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

    let result = kademlia::Node::new(true, kademlia::KADEMLIA_SERVER_ADDR.as_bytes(), kademlia::KADEMLIA_SERVER_PORT, test);
    match result {
        Ok(node_user) => {
            let sleep_time = time::Duration::from_millis(1000);
            thread::sleep(sleep_time);
            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());

                while let Err(result) = node_user.store(&key, &i.to_le_bytes()) {
                    warn!("store result: {:?}", result);
                    thread::sleep(sleep_time);
                }
                info!("node store ok");

                thread::sleep(common::TEST_STORE_INTERVAL);
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

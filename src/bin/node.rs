extern crate kademlia;
extern crate rand;
extern crate env_logger;
extern crate log;
extern crate clap;

use rand::Rng;
use std::{thread, time};
use log::{warn, info, debug, error};
use clap::{Arg, App};

mod common;

fn main() -> std::io::Result<()> {
    env_logger::init();

    let matches = App::new("node")
        .arg(Arg::with_name("test")
             .short("t")
             .long("test")
             .help("run in test mode")
             .takes_value(false))
        .get_matches();
    info!("Init node");

    let test = matches.is_present("test");
    info!("test={}", test);
    let result = kademlia::Node::new(false, kademlia::KADEMLIA_SERVER_ADDR.as_bytes(), kademlia::KADEMLIA_SERVER_PORT, test);
    match result {
        Ok(node_user) => {
            debug!("key_len={:?}", node_user.key_len);
            let key:Vec<u8> = rand::thread_rng().sample_iter(&rand::distributions::Standard).take(node_user.key_len).collect();

            let sleep_time = time::Duration::from_millis(1000);
            while let Err(result) = node_user.store(&key, b"value0") {
                debug!("store result: {:?}", result);
                thread::sleep(sleep_time);
            }
            info!("node store ok");

            for i in 0u32.. {
                let key = common::hash(&i.to_le_bytes());
                while let Err(result) = node_user.find_value(&key) {
                    warn!("find_value failed: i={}, {:?}", i, result);
                    thread::sleep(2*common::TEST_STORE_INTERVAL);
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

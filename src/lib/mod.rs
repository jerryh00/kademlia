extern crate chrono;
extern crate circular_queue;
extern crate derivative;
extern crate log;
extern crate rand;
extern crate rand_distr;
extern crate serde_json;
extern crate timer;

use self::chrono::Utc;
use self::circular_queue::CircularQueue;
use self::derivative::Derivative;
use self::log::{debug, error, info, trace, warn};
use self::rand::{thread_rng, Rng};
use self::rand_distr::{Distribution, Normal};
use self::serde_json::json;
use std::ascii::escape_default;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Result;
use std::iter;
use std::net::{SocketAddr, UdpSocket};
use std::str;
use std::sync::mpsc;
use std::thread;

mod node_tree;
use node_tree::*;

mod bits;
mod common;

use common::*;

#[derive(Debug, Clone, Copy)]
struct ProtoParams {
    id_len: usize,
    rpc_id_len: usize,
    rpc_timeout: i64,
    republish_interval: i64,
    refresh_interval: i64,
    alpha: u32,
    bucket_len: usize,

    test_mode: bool,
    drop_probability: f64,
    delay_avg: f64,
    delay_stddev: f64,
}

const PROTO_PARAMS: ProtoParams = ProtoParams {
    id_len: 32,
    rpc_id_len: 20,
    rpc_timeout: 5,
    republish_interval: 60 * 60 * 24,
    refresh_interval: 60 * 60,
    alpha: 3,
    bucket_len: 20,

    test_mode: false,
    drop_probability: 0.0,
    delay_avg: 0.0,
    delay_stddev: 0.0,
};

const PROTO_PARAMS_TEST: ProtoParams = ProtoParams {
    id_len: 32,
    rpc_id_len: 8,
    rpc_timeout: 3,
    republish_interval: 600,
    refresh_interval: 700,
    alpha: 3,
    bucket_len: 10,

    test_mode: true,
    drop_probability: 0.1,
    delay_avg: 0.1,
    delay_stddev: 0.1,
};

pub struct Node {
    pub key_len: usize,
    to_main_tx: mpsc::Sender<Message>,
    main_to_user_rx: mpsc::Receiver<Result<Vec<u8>>>,

    num_store_ok: Cell<i64>,
    store_time_total: Cell<i64>,
    store_time_avg: Cell<i64>,
    num_findvalue_ok: Cell<i64>,
    findvalue_time_total: Cell<i64>,
    findvalue_time_avg: Cell<i64>,
}

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Clone)]
struct NodeRespStat {
    rpc_id: Vec<u8>,
    resp_stat: RespStat,

    #[derivative(Debug = "ignore")]
    guard: Option<timer::Guard>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum RespStat {
    Init,
    Sent,
    Received,
    ReceivedFailure,
    Timeout,
}

impl Node {
    pub fn new(is_server: bool, server_addr: &[u8], server_port: u16, test: bool) -> Result<Self> {
        let (to_main_tx, to_main_rx) = mpsc::channel();
        let (main_to_user_tx, main_to_user_rx) = mpsc::channel();

        let mut proto_params;
        if test {
            proto_params = PROTO_PARAMS_TEST;
            proto_params.delay_avg = thread_rng().gen_range(0.0..1.0);
            proto_params.delay_stddev = thread_rng().gen_range(0.0..proto_params.delay_avg);
        } else {
            proto_params = PROTO_PARAMS;
        }
        let result = NodeMain::new(
            proto_params,
            is_server,
            server_addr,
            server_port,
            to_main_tx.clone(),
            to_main_rx,
            main_to_user_tx,
        );
        match result {
            Ok(node_main) => {
                thread::Builder::new()
                    .name("main_thread".into())
                    .spawn(move || {
                        let result = NodeMain::main_func(node_main);
                        match result {
                            Ok(_) => (),
                            Err(err) => {
                                error!("main_thread failed: {:?}", err);
                            }
                        };
                    })
                    .expect("thread spawn() should not fail");

                Ok(Node {
                    key_len: proto_params.id_len,
                    to_main_tx,
                    main_to_user_rx,
                    num_store_ok: Cell::new(0),
                    store_time_total: Cell::new(0),
                    store_time_avg: Cell::new(0),
                    num_findvalue_ok: Cell::new(0),
                    findvalue_time_total: Cell::new(0),
                    findvalue_time_avg: Cell::new(0),
                })
            }
            Err(err) => {
                error!("kademlia::Node::new failed: {:?}", err);
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Node new() failed",
                ))
            }
        }
    }

    pub fn store(&self, key: &[u8], value: &[u8]) -> Result<()> {
        info!("Average store time: {} ms", self.store_time_avg.get());
        let begin_timestamp = Utc::now().timestamp_millis();

        if key.len() != self.key_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid key length",
            ));
        }
        let message = Message::UserCmdStore(key.to_vec(), value.to_vec());
        if self.to_main_tx.send(message).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "store send failed",
            ));
        }
        if let Ok(result) = self.main_to_user_rx.recv() {
            self.num_store_ok.set(self.num_store_ok.get() + 1);
            let store_time = Utc::now().timestamp_millis() - begin_timestamp;
            self.store_time_total
                .set(self.store_time_total.get() + store_time);
            self.store_time_avg
                .set(self.store_time_total.get() / self.num_store_ok.get());
            result.map(|_| ())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "store recv failed",
            ))
        }
    }

    pub fn find_value(&self, key: &[u8]) -> Result<Vec<u8>> {
        info!(
            "Average findvalue time: {} ms",
            self.findvalue_time_avg.get()
        );
        let begin_timestamp = Utc::now().timestamp_millis();

        if key.len() != self.key_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid key length",
            ));
        }
        let message = Message::UserCmdFindValue(key.to_vec());
        if self.to_main_tx.send(message).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "find_value send failed",
            ));
        }
        if let Ok(result) = self.main_to_user_rx.recv() {
            self.num_findvalue_ok.set(self.num_findvalue_ok.get() + 1);
            let findvalue_time = Utc::now().timestamp_millis() - begin_timestamp;
            self.findvalue_time_total
                .set(self.findvalue_time_total.get() + findvalue_time);
            self.findvalue_time_avg
                .set(self.findvalue_time_total.get() / self.num_findvalue_ok.get());
            result
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "find_value recv failed",
            ))
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct NodeMain {
    proto_params: ProtoParams,

    is_server: bool,
    id: Vec<u8>,
    socket: UdpSocket,
    tree: Box<NodeTree>,

    to_main_tx: mpsc::Sender<Message>,
    to_main_rx: mpsc::Receiver<Message>,
    main_to_user_tx: mpsc::Sender<Result<Vec<u8>>>,

    status: NodeMainStatus,

    target_node_id: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,

    msg_buf: VecDeque<Message>,

    /* ping is different from other commands, it works without regard to node main status */
    /* HashMap<ping_dest_node, (replacement, node_resp_stat)> */
    ping_resp_stat: HashMap<NodeEntry, (NodeEntry, NodeRespStat)>,

    cmd_dest_resp: HashMap<(Vec<u8>, NodeEntry), NodeRespStat>,
    find_node_resp_stat: BTreeMap<Distance, (NodeEntry, RespStat)>,
    store_resp_stat: BTreeMap<Distance, (NodeEntry, RespStat)>,
    find_value_resp_stat: BTreeMap<Distance, (NodeEntry, RespStat)>,
    store_is_from_user: bool,

    data_store: HashMap<Vec<u8>, (Vec<u8>, i64)>,

    #[derivative(Debug = "ignore")]
    timer: timer::Timer,
    #[derivative(Debug = "ignore")]
    republish_guard: Option<timer::Guard>,
    #[derivative(Debug = "ignore")]
    refresh_guard: Option<timer::Guard>,

    num_net_datagram: u64,
    num_user_cmd_store: u64,
    num_user_cmd_findvalue: u64,
    num_timeout: u64,
    num_lookup: u64,
    num_republish: u64,
    num_republish_single: u64,
    num_refresh: u64,
    num_total_msg: u64,
}

type Distance = Vec<u8>;

#[derive(Copy, Clone, PartialEq, Debug)]
enum NodeMainStatus {
    Idle,
    Lookup,
    StoreLookup,
    StoreStore,
    FindValue,
}

#[derive(Debug)]
enum Message {
    UserCmdStore(Vec<u8>, Vec<u8>),
    UserCmdFindValue(Vec<u8>),
    NetDatagram(Vec<u8>, SocketAddr),
    Timeout(Vec<u8>, NodeEntry),
    Lookup(Vec<u8>),
    Republish,
    RepublishSingle(Vec<u8>, Vec<u8>),
    Refresh,
}

fn gen_server_id(id_len: usize) -> Vec<u8> {
    iter::repeat(0).take(id_len).collect()
}

fn gen_node_id(id_len: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Standard)
        .take(id_len)
        .collect()
}

impl NodeMain {
    pub fn new(
        proto_params: ProtoParams,
        is_server: bool,
        server_addr: &[u8],
        server_port: u16,
        to_main_tx: mpsc::Sender<Message>,
        to_main_rx: mpsc::Receiver<Message>,
        main_to_user_tx: mpsc::Sender<Result<Vec<u8>>>,
    ) -> Result<Self> {
        let port = if is_server { server_port } else { 0 };

        let server_id = gen_server_id(proto_params.id_len);
        let id = if is_server {
            server_id.clone()
        } else {
            gen_node_id(proto_params.id_len)
        };
        let mut tree = Box::new(NodeTree::new(proto_params.bucket_len, &id));
        let server_entry = Box::new(NodeEntry {
            addr: server_addr.to_vec(),
            port: server_port,
            id: server_id,
        });
        tree.put_node(*server_entry)
            .expect("Server node entry insertion should not fail");

        let socket = UdpSocket::bind((
            std::str::from_utf8(server_addr).expect("server address should be a utf8 string"),
            port,
        ))?;

        let _socket = socket
            .try_clone()
            .expect("socket try_clone() should not fail");
        let _to_main_tx = to_main_tx.clone();
        thread::Builder::new()
            .name("network_thread".into())
            .spawn(move || {
                let result = NodeMain::network_func(_socket, _to_main_tx, proto_params);
                match result {
                    Ok(_) => (),
                    Err(err) => {
                        error!("network_thread failed: {:?}", err);
                    }
                };
            })
            .expect("thread spawn() should not fail");

        let node_main = NodeMain {
            proto_params,
            is_server,
            id,
            socket,
            tree,
            to_main_tx,
            to_main_rx,
            main_to_user_tx,
            status: NodeMainStatus::Idle,

            target_node_id: Vec::<u8>::new(),
            key: Vec::<u8>::new(),
            value: Vec::<u8>::new(),

            msg_buf: VecDeque::<Message>::new(),

            cmd_dest_resp: HashMap::new(),
            ping_resp_stat: HashMap::new(),
            find_node_resp_stat: BTreeMap::new(),
            store_resp_stat: BTreeMap::new(),
            find_value_resp_stat: BTreeMap::new(),
            store_is_from_user: true,

            data_store: HashMap::new(),
            timer: timer::Timer::new(),
            republish_guard: None,
            refresh_guard: None,

            num_net_datagram: 0,
            num_user_cmd_store: 0,
            num_user_cmd_findvalue: 0,
            num_timeout: 0,
            num_lookup: 0,
            num_republish: 0,
            num_republish_single: 0,
            num_refresh: 0,
            num_total_msg: 0,
        };

        debug!("{:?}", node_main);
        Ok(node_main)
    }

    fn main_func(mut node: NodeMain) -> Result<()> {
        if !node.is_server {
            node.msg_buf.push_back(Message::Lookup(node.id.clone()));
            node.msg_buf.push_back(Message::Refresh);
        }

        let _to_main_tx = node.to_main_tx.clone();
        let _guard = node.timer.schedule_repeating(
            chrono::Duration::seconds(node.proto_params.republish_interval),
            move || {
                let message = Message::Republish;
                let _ignored = _to_main_tx.send(message);
            },
        );
        node.republish_guard = Some(_guard);

        let _to_main_tx = node.to_main_tx.clone();
        let _guard = node.timer.schedule_repeating(
            chrono::Duration::seconds(node.proto_params.refresh_interval),
            move || {
                let message = Message::Refresh;
                let _ignored = _to_main_tx.send(message);
            },
        );
        node.refresh_guard = Some(_guard);

        loop {
            while node.status == NodeMainStatus::Idle {
                debug!("msg_buf len={}", node.msg_buf.len());
                if let Some(message) = node.msg_buf.pop_front() {
                    if let Err(err) = node.handle_message(message) {
                        debug!("handle_message failed: {:?}", err);
                    }
                } else {
                    break;
                }
            }
            if let Ok(message) = node.to_main_rx.recv() {
                if let Err(err) = node.handle_message(message) {
                    debug!("handle_message failed: {:?}", err);
                }
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "main_func recv failed",
                ));
            }
            node.print_stat();
        }
    }

    fn network_func(
        socket: UdpSocket,
        to_main_tx: mpsc::Sender<Message>,
        proto_params: ProtoParams,
    ) -> Result<()> {
        if proto_params.test_mode {
            let delay_timer = timer::Timer::new();

            /*
             * If datagram traffic is too high, the circular guard buffer would overflow, old
             * pending guard would be overwritten and the assoicated datagram would not be sent
             */
            let max_num_pending_guard = 300; /* Empirical value */
            let mut guard_store = CircularQueue::with_capacity(max_num_pending_guard);

            let normal = Normal::new(proto_params.delay_avg, proto_params.delay_stddev).unwrap();

            loop {
                let (recv, src) = read_message_udp(&socket)?;

                if thread_rng().gen_bool(proto_params.drop_probability) {
                    debug!("Dropping datagram for test");
                    continue;
                }

                let mut delay = normal.sample(&mut rand::thread_rng()) * 1e6;
                if delay < 0.0 {
                    delay = 0.0
                };
                debug!("Adding delay={} us to datagram for test", delay);
                let _to_main_tx = to_main_tx.clone();
                let _guard = delay_timer.schedule_with_delay(
                    chrono::Duration::microseconds(delay as i64),
                    move || {
                        let message = Message::NetDatagram(recv.clone(), src);
                        if let Err(err) = _to_main_tx.send(message) {
                            error!("send fail: {:?}", err);
                        }
                    },
                );
                guard_store.push(_guard);
            }
        } else {
            loop {
                let (recv, src) = read_message_udp(&socket)?;

                let message = Message::NetDatagram(recv, src);
                if let Err(err) = to_main_tx.send(message) {
                    error!("send fail: {:?}", err);
                }
            }
        }
    }

    fn print_stat(&self) {
        if self.num_total_msg % 100 == 0 {
            info!("num_net_datagram={}", self.num_net_datagram);
            info!("num_user_cmd_store={}", self.num_user_cmd_store);
            info!("num_user_cmd_findvalue={}", self.num_user_cmd_findvalue);
            info!("num_timeout={}", self.num_timeout);
            info!("num_lookup={}", self.num_lookup);
            info!("num_republish={}", self.num_republish);
            info!("num_republish_single={}", self.num_republish_single);
            info!("num_refresh={}", self.num_refresh);
            info!("num_total_msg={}", self.num_total_msg);
        }
    }

    fn handle_net_datagram(&mut self, message: Message) -> Result<()> {
        if let Message::NetDatagram(recv, src) = message {
            self.num_net_datagram += 1;
            debug!("src={:?}", src);
            let recv_str;
            if let Ok(result) = std::str::from_utf8(&recv) {
                recv_str = result;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "datagram is not utf8 string",
                ));
            }
            if let Ok(serde_json::Value::Object(msg)) = serde_json::from_str(recv_str) {
                trace!("received in json: {:?}", msg);

                let node_id: Vec<u8>;
                if let Some(object) = msg.get(NODE_ID) {
                    node_id = serde_json::from_value(object.clone()).unwrap();
                    debug!("node_id={:?}", node_id);
                    let node_entry = NodeEntry {
                        addr: src.ip().to_string().into_bytes(),
                        port: src.port(),
                        id: node_id.clone(),
                    };
                    if let Err(old_node_entry) = self.tree.put_node(node_entry.clone()) {
                        debug!(
                            "put_node failed, to ping old node_entry: {:?}",
                            &old_node_entry
                        );
                        if let Err(err) = self.ping(&old_node_entry, &node_entry) {
                            error!("ping failed: {:?}", err);
                        }
                    }
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "no node id in datagram",
                    ));
                }

                if let Some(serde_json::Value::String(function)) = msg.get(FUNCTION) {
                    if function == PING {
                        self.handle_ping(&msg, &src, &node_id)?
                    } else if function == PING_ACK {
                        self.handle_ping_ack(&msg, &src, &node_id)?
                    } else if function == FIND_NODE {
                        self.handle_find_node(&msg, &src, &node_id)?
                    } else if function == FIND_NODE_ACK {
                        self.handle_find_node_ack(&msg, &src, &node_id)?
                    } else if function == STORE {
                        self.handle_store_req(&msg, &src, &node_id)?
                    } else if function == STORE_ACK {
                        self.handle_store_ack(&msg, &src, &node_id)?
                    } else if function == FIND_VALUE {
                        self.handle_find_value(&msg, &src, &node_id)?
                    } else if function == FIND_VALUE_ACK {
                        self.handle_find_value_ack(&msg, &src, &node_id)?
                    }
                } else {
                    error!("no function in datagram, recv_str: {:?}", recv_str);
                }
            } else {
                error!("serde_json::from_str failed, recv_str: {:?}", recv_str);
            }
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_user_cmd_store(&mut self, message: Message) -> Result<()> {
        if let Message::UserCmdStore(key, value) = message {
            self.num_user_cmd_store += 1;
            debug!("node main status: {:?}", self.status);
            if self.status != NodeMainStatus::Idle {
                self.msg_buf.push_back(Message::UserCmdStore(key, value));
            } else {
                self.set_status(NodeMainStatus::StoreLookup);
                self.store_is_from_user = true;

                info!(
                    "UserCmdStore: key={}, value={}",
                    show_buf(&key),
                    show_buf(&value)
                );
                if let Err(err) = self.store(&key, &value) {
                    self.main_to_user_tx
                        .send(Err(err))
                        .expect("channel send() should not fail");
                }
            }

            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_user_cmd_findvalue(&mut self, message: Message) -> Result<()> {
        if let Message::UserCmdFindValue(key) = message {
            self.num_user_cmd_findvalue += 1;
            if self.status != NodeMainStatus::Idle {
                debug!("FindValue: status is not idle, push command to message buffer");
                self.msg_buf.push_back(Message::UserCmdFindValue(key));
            } else {
                self.set_status(NodeMainStatus::FindValue);
                debug!("FindValue: key={}", show_buf(&key));

                if let Err(err) = self.find_value(&key) {
                    self.main_to_user_tx
                        .send(Err(err))
                        .expect("channel send() should not fail");
                }
            }

            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_storelookup_over(&mut self) {
        let node_entries: Vec<NodeEntry> = self
            .find_node_resp_stat
            .values()
            .take(self.proto_params.bucket_len)
            .filter(|(_, resp_stat)| *resp_stat == RespStat::Received)
            .cloned()
            .map(|(node_entry, _)| node_entry)
            .collect();
        if node_entries.is_empty() {
            self.main_to_user_tx
                .send(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "store failed: no dest nodes available",
                )))
                .expect("channel send() should not fail");
            self.set_status(NodeMainStatus::Idle);
            return;
        }

        NodeMain::update_node_resp_stat(
            &self.key,
            node_entries.clone(),
            &mut self.store_resp_stat,
            RespStat::Sent,
        );
        let key = self.key.clone();
        let value = self.value.clone();
        self.set_status(NodeMainStatus::StoreStore);
        for node_entry in node_entries {
            if let Err(err) = self.store_req(&node_entry, &key, &value) {
                error!("store_req failed: {:?}", err);
            }
        }
    }

    fn handle_timeout_find_node(&mut self, node_entry: NodeEntry) -> Result<()> {
        debug!("FIND_NODE timeout");
        let distance = bits::get_distance(&node_entry.id, &self.target_node_id);
        if let Some((_, resp_stat)) = self.find_node_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Timeout;
        }

        if self.try_find_node_req().is_ok() {
            return Ok(());
        }

        if self.status == NodeMainStatus::Lookup {
            trace!(
                "Ending lookup_node, find_node_resp_stat: {:?}",
                &self.find_node_resp_stat
            );
            self.set_status(NodeMainStatus::Idle);
        } else if self.status == NodeMainStatus::StoreLookup {
            self.handle_storelookup_over();
        }
        Ok(())
    }

    fn handle_timeout_store(&mut self, node_entry: NodeEntry) -> Result<()> {
        debug!("STORE timeout");
        let distance = bits::get_distance(&node_entry.id, &self.key);
        if let Some((_, resp_stat)) = self.store_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Timeout;
        }

        if NodeMain::have_pending_requests(&self.store_resp_stat, self.proto_params.bucket_len) {
            debug!("Wait for pending requests results");
            return Ok(());
        }

        trace!("Ending store, store_resp_stat: {:?}", self.store_resp_stat);
        if self.store_is_from_user {
            if self.is_store_complete() {
                self.main_to_user_tx
                    .send(Ok(b"success".to_vec()))
                    .expect("channel send() should not fail");
            } else {
                self.main_to_user_tx
                    .send(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "store timeout",
                    )))
                    .expect("channel send() should not fail");
            }
        }
        self.set_status(NodeMainStatus::Idle);
        Ok(())
    }

    fn handle_timeout_find_value(&mut self, node_entry: NodeEntry) -> Result<()> {
        debug!("FIND_VALUE timeout");
        let distance = bits::get_distance(&node_entry.id, &self.key);
        if let Some((_, resp_stat)) = self.find_value_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Timeout;
        }
        if self.try_find_value_req().is_ok() {
            return Ok(());
        }

        debug!("Ending find_value");
        let result = self.main_to_user_tx.send(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "find_value timeout",
        )));
        debug!("send result: {:?}", result);
        self.set_status(NodeMainStatus::Idle);
        Ok(())
    }

    fn handle_timeout_ping(&mut self, node_entry: NodeEntry) -> Result<()> {
        debug!("PING timeout");
        let _replacement;
        if let Some((replacement, _)) = self.ping_resp_stat.get(&node_entry) {
            _replacement = replacement.clone();
        } else {
            return Ok(());
        }

        debug!("Removing node: {:?}", &node_entry);
        self.tree.remove_node(&node_entry.id);
        if let Err(old_node_entry) = self.tree.put_node(_replacement.clone()) {
            debug!(
                "put_node failed, to ping old node_entry: {:?}",
                &old_node_entry
            );
            if let Err(err) = self.ping(&old_node_entry, &_replacement) {
                error!("ping failed: {:?}", err);
            }
        }

        self.ping_resp_stat.remove(&node_entry);
        trace!("ping_resp_stat: {:?}", self.ping_resp_stat);
        Ok(())
    }

    fn handle_timeout(&mut self, message: Message) -> Result<()> {
        if let Message::Timeout(cmd, node_entry) = message {
            self.num_timeout += 1;
            debug!(
                "Timeout: cmd={}, node_entry={:?}",
                show_buf(&cmd),
                node_entry
            );
            if str::from_utf8(&cmd).unwrap() == PING {
                if let Some((_, node_resp_stat)) = self.ping_resp_stat.get_mut(&node_entry) {
                    node_resp_stat.resp_stat = RespStat::Timeout;
                } else {
                    debug!("bogus ping message timeout");
                    return Ok(());
                }
            }

            if let Some(node_resp_stat) = self
                .cmd_dest_resp
                .get_mut(&(cmd.clone(), node_entry.clone()))
            {
                node_resp_stat.resp_stat = RespStat::Timeout;
            } else {
                debug!("bogus message timeout");
                return Ok(());
            }

            match str::from_utf8(&cmd).unwrap() {
                FIND_NODE => self.handle_timeout_find_node(node_entry),
                STORE => self.handle_timeout_store(node_entry),
                FIND_VALUE => self.handle_timeout_find_value(node_entry),
                PING => self.handle_timeout_ping(node_entry),
                &_ => {
                    warn!("unrecognized cmd: {:?}", cmd);
                    Ok(())
                }
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_lookup(&mut self, message: Message) -> Result<()> {
        if let Message::Lookup(node_id) = message {
            self.num_lookup += 1;
            debug!("Lookup: node_id={}", show_buf(&node_id));

            self.set_status(NodeMainStatus::Lookup);
            if let Err(err) = self.lookup_node(&node_id) {
                error!("lookup_node failed: {:?}", err);
            }
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_republish(&mut self, message: Message) -> Result<()> {
        self.num_republish += 1;
        match message {
            Message::Republish => {
                debug!("Republish");
                for (key, (value, timestamp)) in &self.data_store {
                    let now = Utc::now().timestamp();
                    if now - timestamp > self.proto_params.republish_interval {
                        self.msg_buf
                            .push_back(Message::RepublishSingle(key.to_vec(), value.to_vec()));
                    }
                }
                Ok(())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            )),
        }
    }

    fn handle_refresh(&mut self, message: Message) -> Result<()> {
        self.num_refresh += 1;
        match message {
            Message::Refresh => {
                debug!("Refresh");
                for prefix in self.tree.get_inactive_kbuckets_prefix(
                    chrono::Utc::now().timestamp() - self.proto_params.refresh_interval,
                ) {
                    let node_id = bits::gen_random_id_in_bucket(&prefix, self.proto_params.id_len);
                    debug!(
                        "prefix: {:?}, random node_id={}",
                        &prefix,
                        show_buf(&node_id)
                    );
                    self.msg_buf.push_back(Message::Lookup(node_id));
                }
                Ok(())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            )),
        }
    }

    fn handle_republish_single(&mut self, message: Message) -> Result<()> {
        if let Message::RepublishSingle(key, value) = message {
            self.num_republish_single += 1;
            debug!(
                "RepublishSingle: key={}, value={:?}",
                show_buf(&key),
                show_buf(&value)
            );

            self.set_status(NodeMainStatus::StoreLookup);
            self.store_is_from_user = false;

            if self.store(&key, &value).is_err() {
                warn!(
                    "RepublishSingle failed: key={}, value={:?}",
                    show_buf(&key),
                    show_buf(&value)
                );
            }

            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "message type error",
            ))
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<()> {
        self.num_total_msg += 1;
        match message {
            m @ Message::NetDatagram(..) => self.handle_net_datagram(m),
            m @ Message::UserCmdStore(..) => self.handle_user_cmd_store(m),
            m @ Message::UserCmdFindValue(..) => self.handle_user_cmd_findvalue(m),
            m @ Message::Timeout(..) => self.handle_timeout(m),
            m @ Message::Lookup(..) => self.handle_lookup(m),
            m @ Message::Republish => self.handle_republish(m),
            m @ Message::RepublishSingle(..) => self.handle_republish_single(m),
            m @ Message::Refresh => self.handle_refresh(m),
        }
    }

    fn handle_ping(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got ping: {}", src);
        if let Some(object) = msg.get(RPC_ID) {
            let rpc_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            self.ping_ack(node_id, &rpc_id, src)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no rpc id in ping",
            ))
        }
    }

    fn compare_rpc_id(
        rpc_id_sent: &[u8],
        msg: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        if let Some(object) = msg.get(RPC_ID) {
            let rpc_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            if rpc_id != rpc_id_sent {
                debug!(
                    "incorrect rpc id in ack, expecting: {:?}, got: {:?}",
                    rpc_id_sent, rpc_id
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "incorrect rpc id in ack",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no rpc id in ack",
            ));
        }

        Ok(())
    }

    fn handle_ping_ack(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got ping ack: {}", src);
        debug!("node_id={:?}", node_id);

        let node_entry = NodeEntry {
            addr: src.ip().to_string().into_bytes(),
            port: src.port(),
            id: node_id.to_owned(),
        };
        if let Some((_, node_resp_stat)) = self.ping_resp_stat.get_mut(&node_entry) {
            let result = NodeMain::compare_rpc_id(&node_resp_stat.rpc_id, msg);
            if result.is_ok() {
                node_resp_stat.resp_stat = RespStat::Received;
                node_resp_stat.guard = None;
            } else {
                debug!(
                    "handle_ping_ack: compare_rpc_id failed {:?}, src={}",
                    result, src
                );
                return Ok(());
            }
        } else {
            debug!("bogus ping_ack");
            return Ok(());
        }

        /* By put_node(), Update node_entry's kbucket position to be most-recently seen */
        if let Err(old_node_entry) = self.tree.put_node(node_entry.clone()) {
            debug!(
                "put_node failed, to ping old node_entry: {:?}",
                &old_node_entry
            );
            if let Err(err) = self.ping(&old_node_entry, &node_entry) {
                error!("ping failed: {:?}", err);
            }
        }

        self.ping_resp_stat.remove(&node_entry);
        trace!("ping_resp_stat: {:?}", self.ping_resp_stat);

        Ok(())
    }

    fn handle_find_value(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got find_value request: {}", src);
        debug!("node_id={:?}", node_id);
        if let Some(object) = msg.get(RPC_ID) {
            let rpc_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            if let Some(object) = msg.get(KEY) {
                let key: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
                self.find_value_ack(node_id, &rpc_id, &key, src)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "no key in find_value request",
                ))
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no rpc id in find_value request",
            ))
        }
    }

    fn handle_find_node(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got find_node request: {}", src);
        debug!("node_id={:?}", node_id);
        if let Some(object) = msg.get(RPC_ID) {
            let rpc_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            if let Some(object) = msg.get(TARGET_NODE_ID) {
                let target_node_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
                self.find_node_ack(node_id, &rpc_id, &target_node_id, src)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "no target node id in find_node request",
                ))
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no rpc id in find_node request",
            ))
        }
    }

    fn check_rpc_id(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        function: Vec<u8>,
        node_entry: &NodeEntry,
    ) -> Result<()> {
        if let Some(node_resp_stat) = self
            .cmd_dest_resp
            .get_mut(&(function.clone(), node_entry.clone()))
        {
            let result = NodeMain::compare_rpc_id(&node_resp_stat.rpc_id, msg);
            if result.is_ok() {
                node_resp_stat.resp_stat = RespStat::Received;
                node_resp_stat.guard = None;
            }
            result
        } else {
            debug!(
                "bogus ack: function: {}, node_entry: {:?}",
                show_buf(function),
                node_entry
            );
            Err(std::io::Error::new(std::io::ErrorKind::Other, "bogus ack"))
        }
    }

    fn handle_find_node_ack(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got find node ack: {}", src);
        debug!("node_id={:?}", node_id);

        let node_entry = NodeEntry {
            addr: src.ip().to_string().into_bytes(),
            port: src.port(),
            id: node_id.to_vec(),
        };
        if self
            .check_rpc_id(msg, FIND_NODE.as_bytes().to_vec(), &node_entry)
            .is_err()
        {
            return Ok(());
        }

        let distance = bits::get_distance(node_id, &self.target_node_id);
        if let Some((_, resp_stat)) = self.find_node_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Received;
        }

        if let Some(object) = msg.get(NODE_ENTRIES) {
            let node_entries: Vec<NodeEntry> = serde_json::from_value(object.clone()).unwrap();
            trace!("node_entries: {:?}", node_entries);

            NodeMain::update_node_resp_stat(
                &self.target_node_id,
                node_entries,
                &mut self.find_node_resp_stat,
                RespStat::Init,
            );
            if self.try_find_node_req().is_ok() {
                return Ok(());
            }

            if self.status == NodeMainStatus::Lookup {
                trace!(
                    "Ending lookup_node, find_node_resp_stat: {:?}",
                    &self.find_node_resp_stat
                );
                self.set_status(NodeMainStatus::Idle);
            } else if self.status == NodeMainStatus::StoreLookup {
                self.handle_storelookup_over();
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no node entries in find_node ack",
            ));
        }

        Ok(())
    }

    fn handle_find_value_ack(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got find value ack: {}", src);
        debug!("node_id={:?}", node_id);

        let node_entry = NodeEntry {
            addr: src.ip().to_string().into_bytes(),
            port: src.port(),
            id: node_id.to_vec(),
        };
        if self
            .check_rpc_id(msg, FIND_VALUE.as_bytes().to_vec(), &node_entry)
            .is_err()
        {
            return Ok(());
        }

        let distance = bits::get_distance(node_id, &self.key);
        if let Some((_, resp_stat)) = self.find_value_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Received;
        }

        if let Some(object) = msg.get(VALUE) {
            let value: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            trace!("value: {:?}", value);

            let result = self.main_to_user_tx.send(Ok(value));
            debug!("send result: {:?}", result);
            self.set_status(NodeMainStatus::Idle);
            return Ok(());
        } else {
            debug!("no value in find_value_ack");
        }

        if let Some(object) = msg.get(NODE_ENTRIES) {
            let node_entries: Vec<NodeEntry> = serde_json::from_value(object.clone()).unwrap();
            trace!("node_entries: {:?}", node_entries);

            NodeMain::update_node_resp_stat(
                &self.target_node_id,
                node_entries,
                &mut self.find_value_resp_stat,
                RespStat::Init,
            );
            if self.try_find_value_req().is_ok() {
                return Ok(());
            }

            debug!(
                "Ending lookup_value, find_value_resp_stat: {:?}",
                self.find_value_resp_stat
            );
            let result = self.main_to_user_tx.send(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "value not found",
            )));
            debug!("send result: {:?}", result);
            self.set_status(NodeMainStatus::Idle);
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no node entries in find_value ack",
            ));
        }

        Ok(())
    }

    fn handle_store_req(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got store request: {}", src);
        debug!("node_id={:?}", node_id);
        if let Some(object) = msg.get(RPC_ID) {
            let rpc_id: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
            if let Some(object) = msg.get(KEY) {
                let key: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
                if let Some(object) = msg.get(VALUE) {
                    let value: Vec<u8> = serde_json::from_value(object.clone()).unwrap();
                    self.store_ack(node_id, &rpc_id, &key, &value, src)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "no value in store request",
                    ))
                }
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "no key in store request",
                ))
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no rpc id in store request",
            ))
        }
    }

    fn is_store_complete(&self) -> bool {
        self.store_resp_stat
            .iter()
            .take(self.proto_params.bucket_len)
            .filter(|(_, (_, resp_stat))| *resp_stat == RespStat::Received)
            .count()
            >= 1
    }

    fn handle_store_ack(
        &mut self,
        msg: &serde_json::Map<String, serde_json::Value>,
        src: &SocketAddr,
        node_id: &[u8],
    ) -> Result<()> {
        debug!("Got store ack: {}", src);
        debug!("node_id={:?}", node_id);

        let node_entry = NodeEntry {
            addr: src.ip().to_string().into_bytes(),
            port: src.port(),
            id: node_id.to_vec(),
        };
        if self
            .check_rpc_id(msg, STORE.as_bytes().to_vec(), &node_entry)
            .is_err()
        {
            return Ok(());
        }

        if let Some(object) = msg.get(ACK) {
            let ack: String = serde_json::from_value(object.clone()).unwrap();
            if ack == OK {
                let distance = bits::get_distance(node_id, &self.key);
                if let Some((_, resp_stat)) = self.store_resp_stat.get_mut(&distance) {
                    *resp_stat = RespStat::Received;
                }

                if NodeMain::have_pending_requests(
                    &self.store_resp_stat,
                    self.proto_params.bucket_len,
                ) {
                    debug!("Wait for pending requests results");
                    return Ok(());
                }

                debug!("Ending store, store_resp_stat: {:?}", self.store_resp_stat);
                if self.store_is_from_user {
                    if self.is_store_complete() {
                        self.main_to_user_tx
                            .send(Ok(b"success".to_vec()))
                            .expect("channel send() should not fail");
                    } else {
                        self.main_to_user_tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "store timeout",
                            )))
                            .expect("channel send() should not fail");
                    }
                }
                self.set_status(NodeMainStatus::Idle);
            } else {
                let distance = bits::get_distance(node_id, &self.key);
                if let Some((_, resp_stat)) = self.store_resp_stat.get_mut(&distance) {
                    *resp_stat = RespStat::ReceivedFailure;
                }
            }
            Ok(())
        } else {
            let distance = bits::get_distance(node_id, &self.key);
            if let Some((_, resp_stat)) = self.store_resp_stat.get_mut(&distance) {
                *resp_stat = RespStat::ReceivedFailure;
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no ack in store ack",
            ))
        }
    }

    fn store_req(&mut self, node_entry: &NodeEntry, key: &[u8], value: &[u8]) -> Result<()> {
        let rpc_id = self.gen_rpc_id();
        let store_req = json!({
            FUNCTION: STORE,
            KEY: key.to_vec(),
            VALUE: value.to_vec(),
            NODE_ID: self.id,
            RPC_ID: rpc_id
        });

        debug!("store dest: {:?}", &node_entry);

        let _node_entry = node_entry.clone();
        let _to_main_tx = self.to_main_tx.clone();
        let _guard = self.timer.schedule_with_delay(
            chrono::Duration::seconds(self.proto_params.rpc_timeout),
            move || {
                let message = Message::Timeout(STORE.as_bytes().to_vec(), _node_entry.clone());
                let _ignored = _to_main_tx.send(message);
            },
        );
        self.cmd_dest_resp.insert(
            (STORE.as_bytes().to_vec(), node_entry.clone()),
            NodeRespStat {
                rpc_id,
                resp_stat: RespStat::Sent,
                guard: Some(_guard),
            },
        );

        write_message_udp(
            &self.socket,
            store_req.to_string().as_bytes(),
            &(SocketAddr::new(
                String::from_utf8(node_entry.addr.clone())
                    .unwrap()
                    .parse()
                    .unwrap(),
                node_entry.port,
            )),
        )
    }

    fn ping(&mut self, node_entry: &NodeEntry, replacement: &NodeEntry) -> Result<()> {
        let rpc_id = self.gen_rpc_id();
        let ping = json!({
            FUNCTION: PING,
            NODE_ID: self.id,
            RPC_ID: rpc_id
        });

        debug!("ping dest: {:?}", &node_entry);

        let _node_entry = node_entry.clone();
        let _to_main_tx = self.to_main_tx.clone();
        let _guard = self.timer.schedule_with_delay(
            chrono::Duration::seconds(self.proto_params.rpc_timeout),
            move || {
                let message = Message::Timeout(PING.as_bytes().to_vec(), _node_entry.clone());
                let _ignored = _to_main_tx.send(message);
            },
        );
        self.ping_resp_stat.insert(
            node_entry.clone(),
            (
                replacement.clone(),
                NodeRespStat {
                    rpc_id,
                    resp_stat: RespStat::Sent,
                    guard: Some(_guard),
                },
            ),
        );

        write_message_udp(
            &self.socket,
            ping.to_string().as_bytes(),
            &(SocketAddr::new(
                String::from_utf8(node_entry.addr.clone())
                    .unwrap()
                    .parse()
                    .unwrap(),
                node_entry.port,
            )),
        )
    }

    fn ping_ack(&self, node: &[u8], rpc_id: &[u8], src: &SocketAddr) -> Result<()> {
        debug!("ping_ack dest: {:?}", node);
        let ping_ack = json!({
            FUNCTION: PING_ACK,
            NODE_ID: self.id,
            RPC_ID: rpc_id
        });
        write_message_udp(&self.socket, ping_ack.to_string().as_bytes(), src)
    }

    fn find_value_req(&mut self, node_entry: &NodeEntry, key: &[u8]) -> Result<()> {
        let rpc_id = self.gen_rpc_id();
        let find_value_req = json!({
            FUNCTION: FIND_VALUE,
            NODE_ID: self.id,
            KEY: key,
            RPC_ID: rpc_id
        });

        debug!("find_value_req dest: {:?}", node_entry);

        let _node_entry = node_entry.clone();
        let _to_main_tx = self.to_main_tx.clone();
        let _guard = self.timer.schedule_with_delay(
            chrono::Duration::seconds(self.proto_params.rpc_timeout),
            move || {
                let message = Message::Timeout(FIND_VALUE.as_bytes().to_vec(), _node_entry.clone());
                let _ignored = _to_main_tx.send(message);
            },
        );
        self.cmd_dest_resp.insert(
            (FIND_VALUE.as_bytes().to_vec(), node_entry.clone()),
            NodeRespStat {
                rpc_id,
                resp_stat: RespStat::Sent,
                guard: Some(_guard),
            },
        );
        let distance = bits::get_distance(&node_entry.id, key);
        if let Some((_, resp_stat)) = self.find_value_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Sent;
        }

        write_message_udp(
            &self.socket,
            find_value_req.to_string().as_bytes(),
            &(SocketAddr::new(
                String::from_utf8(node_entry.addr.clone())
                    .unwrap()
                    .parse()
                    .unwrap(),
                node_entry.port,
            )),
        )
    }

    fn find_node_req(&mut self, node_entry: &NodeEntry, target_node_id: &[u8]) -> Result<()> {
        let rpc_id = self.gen_rpc_id();
        let find_node_req = json!({
            FUNCTION: FIND_NODE,
            NODE_ID: self.id,
            TARGET_NODE_ID: target_node_id,
            RPC_ID: rpc_id
        });

        debug!("find_node_req dest: {:?}", node_entry);

        let _node_entry = node_entry.clone();
        let _to_main_tx = self.to_main_tx.clone();
        let _guard = self.timer.schedule_with_delay(
            chrono::Duration::seconds(self.proto_params.rpc_timeout),
            move || {
                let message = Message::Timeout(FIND_NODE.as_bytes().to_vec(), _node_entry.clone());
                let _ignored = _to_main_tx.send(message);
            },
        );
        self.cmd_dest_resp.insert(
            (FIND_NODE.as_bytes().to_vec(), node_entry.clone()),
            NodeRespStat {
                rpc_id,
                resp_stat: RespStat::Sent,
                guard: Some(_guard),
            },
        );
        let distance = bits::get_distance(&node_entry.id, target_node_id);
        if let Some((_, resp_stat)) = self.find_node_resp_stat.get_mut(&distance) {
            *resp_stat = RespStat::Sent;
        }

        write_message_udp(
            &self.socket,
            find_node_req.to_string().as_bytes(),
            &(SocketAddr::new(
                String::from_utf8(node_entry.addr.clone())
                    .unwrap()
                    .parse()
                    .unwrap(),
                node_entry.port,
            )),
        )
    }

    fn find_node_ack(
        &self,
        node: &[u8],
        rpc_id: &[u8],
        target_node_id: &[u8],
        src: &SocketAddr,
    ) -> Result<()> {
        debug!("find_node_ack dest: {:?}, addr={:?}", node, src);
        let node_entries = self.find_node_local(target_node_id);
        let find_node_ack = json!({
            FUNCTION: FIND_NODE_ACK,
            NODE_ID: self.id,
            NODE_ENTRIES: node_entries,
            RPC_ID: rpc_id
        });
        write_message_udp(&self.socket, find_node_ack.to_string().as_bytes(), src)
    }

    fn find_value_ack(
        &self,
        node: &[u8],
        rpc_id: &[u8],
        key: &[u8],
        src: &SocketAddr,
    ) -> Result<()> {
        debug!("find_value_ack dest: {:?}", node);
        let find_value_ack;
        let node_entries = self.find_node_local(key);
        if let Some((value, _)) = self.data_store.get(key) {
            find_value_ack = json!({
                FUNCTION: FIND_VALUE_ACK,
                NODE_ID: self.id,
                VALUE: value,
                RPC_ID: rpc_id
            });
        } else {
            find_value_ack = json!({
                FUNCTION: FIND_VALUE_ACK,
                NODE_ID: self.id,
                NODE_ENTRIES: node_entries,
                RPC_ID: rpc_id
            });
        }
        write_message_udp(&self.socket, find_value_ack.to_string().as_bytes(), src)
    }

    fn store_ack(
        &mut self,
        node: &[u8],
        rpc_id: &[u8],
        key: &[u8],
        value: &[u8],
        src: &SocketAddr,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        self.data_store.insert(key.to_vec(), (value.to_vec(), now));
        debug!("store_ack dest: {:?}", node);
        let store_ack = json!({
            FUNCTION: STORE_ACK,
            NODE_ID: self.id,
            RPC_ID: rpc_id,
            ACK: OK
        });
        write_message_udp(&self.socket, store_ack.to_string().as_bytes(), src)
    }

    fn store(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.key = key.to_vec();
        self.value = value.to_vec();
        self.lookup_node(key)
    }

    fn find_node_local(&self, node: &[u8]) -> Vec<NodeEntry> {
        self.tree.get_bucket_len_nodes(node)
    }

    fn find_value(&mut self, key: &[u8]) -> Result<()> {
        self.key = key.to_vec();
        self.lookup_value(key)
    }

    fn lookup_value(&mut self, key: &[u8]) -> Result<()> {
        let node_entries = self.find_node_local(key);
        self.target_node_id = key.to_vec();
        NodeMain::update_node_resp_stat(
            &self.target_node_id,
            node_entries,
            &mut self.find_value_resp_stat,
            RespStat::Init,
        );
        self.try_find_value_req()
    }

    fn try_find_value_req(&mut self) -> Result<()> {
        for _ in 0..self.proto_params.alpha {
            if let Some(node_entry) = NodeMain::get_next_init_node(
                &self.find_value_resp_stat,
                self.proto_params.bucket_len,
            ) {
                let _ = self.find_value_req(&node_entry, &self.target_node_id.clone());
            }
        }

        if NodeMain::have_pending_requests(&self.find_value_resp_stat, self.proto_params.bucket_len)
        {
            debug!("Wait for pending requests results");
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No dest node for lookup_value",
            ))
        }
    }

    fn try_find_node_req(&mut self) -> Result<()> {
        for _ in 0..self.proto_params.alpha {
            if let Some(node_entry) = NodeMain::get_next_init_node(
                &self.find_node_resp_stat,
                self.proto_params.bucket_len,
            ) {
                let _ = self.find_node_req(&node_entry, &self.target_node_id.clone());
            }
        }

        if NodeMain::have_pending_requests(&self.find_node_resp_stat, self.proto_params.bucket_len)
        {
            debug!("Wait for pending requests results");
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No dest node for lookup_node",
            ))
        }
    }

    fn lookup_node(&mut self, node: &[u8]) -> Result<()> {
        let node_entries = self.find_node_local(node);

        self.target_node_id = node.to_vec();

        NodeMain::update_node_resp_stat(
            &self.target_node_id,
            node_entries,
            &mut self.find_node_resp_stat,
            RespStat::Init,
        );
        self.try_find_node_req()
    }

    fn gen_rpc_id(&self) -> Vec<u8> {
        let rpc_id: Vec<u8> = rand::thread_rng()
            .sample_iter(&rand::distributions::Standard)
            .take(self.proto_params.rpc_id_len)
            .collect();
        rpc_id
    }

    fn update_node_resp_stat(
        target_node_id: &[u8],
        node_entries: Vec<NodeEntry>,
        stat: &mut BTreeMap<Distance, (NodeEntry, RespStat)>,
        resp_stat: RespStat,
    ) {
        for node_entry in node_entries {
            let distance = bits::get_distance(&node_entry.id, target_node_id);
            stat.entry(distance).or_insert((node_entry, resp_stat));
        }
    }

    fn get_next_init_node(
        stat: &BTreeMap<Distance, (NodeEntry, RespStat)>,
        bucket_len: usize,
    ) -> Option<NodeEntry> {
        for (_, (node_entry, resp_stat)) in stat.iter().take(bucket_len) {
            if *resp_stat == RespStat::Init {
                return Some(node_entry.clone());
            }
        }
        None
    }

    fn have_pending_requests(
        stat: &BTreeMap<Distance, (NodeEntry, RespStat)>,
        bucket_len: usize,
    ) -> bool {
        for (_, (_, resp_stat)) in stat.iter().take(bucket_len) {
            if *resp_stat == RespStat::Sent {
                return true;
            }
        }
        false
    }

    fn set_status(&mut self, status: NodeMainStatus) {
        self.status = status;
        if status == NodeMainStatus::Idle {
            self.target_node_id = Vec::<u8>::new();
            self.key = Vec::<u8>::new();
            self.value = Vec::<u8>::new();
            self.cmd_dest_resp = HashMap::new();
            self.find_node_resp_stat = BTreeMap::new();
            self.store_resp_stat = BTreeMap::new();
            self.find_value_resp_stat = BTreeMap::new();
            self.store_is_from_user = true;
        }
    }
}

pub fn show_buf<B: AsRef<[u8]>>(buf: B) -> String {
    String::from_utf8(
        buf.as_ref()
            .iter()
            .flat_map(|b| escape_default(*b))
            .collect(),
    )
    .unwrap()
}

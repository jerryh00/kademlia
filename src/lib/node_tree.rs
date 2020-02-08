extern crate serde;
extern crate chrono;

use super::bits;
use self::serde::{Serialize, Deserialize};
use super::common::*;
use std::cell::Cell;
use self::chrono::Utc;

#[cfg(test)]
use std::{thread, time};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct NodeEntry {
    pub addr: Vec<u8>,
    pub port: u16,
    pub id: Vec<u8>,
}

#[derive(Debug)]
enum Node {
    Children{left: Box<NodeTree>, right: Box<NodeTree>},
    KBuckets{
        node_entries: Vec<NodeEntry>,
        timestamp: Cell<i64>,
        prefix: Vec<bool>
    },
}

#[derive(Debug)]
pub struct NodeTree {
    node: Node,
    meta_data: Box<TreeMetaData>,
}

#[derive(Debug, Clone)]
struct TreeMetaData {
    bucket_len: usize,
    node_id: Vec<u8>,
}

impl NodeTree {
    pub fn new(bucket_len: usize, node_id: &[u8]) -> NodeTree {
        let meta_data = Box::new(TreeMetaData{bucket_len, node_id: node_id.to_vec()});
        NodeTree {
            node: Node::KBuckets{
                node_entries: Vec::with_capacity(bucket_len),
                timestamp: Cell::new(0),
                prefix: Vec::new(),
            },
            meta_data
        }
    }

    fn id_in_range(prefix: &[bool], node_id: &[u8]) -> bool {
        bits::bytes_to_bits(node_id).starts_with(prefix)
    }

    fn put_node_at_height(tree: &mut NodeTree, node_entry: NodeEntry, height: usize) -> Result<(), NodeEntry> {
        let node_id_bits = bits::bytes_to_bits(&node_entry.id);
        assert!(height <= node_id_bits.len());

        match &mut tree.node {
            Node::Children {left, right} => {
                if *node_id_bits.get(height).unwrap() {
                    NodeTree::put_node_at_height(left, node_entry, height + 1)
                } else {
                    NodeTree::put_node_at_height(right, node_entry, height + 1)
                }
            }
            Node::KBuckets{node_entries, timestamp, prefix} => {
                if let Some(position) = node_entries.iter().position(|entry|compare_collection::<u8>(&entry.id, &node_entry.id)) {
                    node_entries.remove(position);
                }
                if node_entries.len()  < tree.meta_data.bucket_len {
                    node_entries.push(node_entry);
                    timestamp.set(Utc::now().timestamp());
                    Ok(())
                } else if NodeTree::id_in_range(&prefix, &tree.meta_data.node_id) {
                    NodeTree::split_tree(tree, height);
                    NodeTree::put_node_at_height(tree, node_entry, height)
                } else {
                    let old_node_entry = node_entries.first().unwrap().clone();
                    Err(old_node_entry)
                }
            }
        }
    }

    fn split_tree(tree: &mut NodeTree, height: usize) {
        let mut left_buckets = Vec::new();
        let mut right_buckets = Vec::new();

        if let Node::KBuckets{node_entries, timestamp, prefix} = &mut tree.node {
            while let Some(node_entry) = node_entries.pop() {
                let node_id_bits = bits::bytes_to_bits(&node_entry.id);
                if *node_id_bits.get(height).unwrap() {
                    left_buckets.push(node_entry);
                } else {
                    right_buckets.push(node_entry);
                }
            }

            let mut left_prefix = prefix.clone();
            left_prefix.push(true);
            let left = Box::new(NodeTree {
                node: Node::KBuckets{node_entries: left_buckets, timestamp: Cell::new(timestamp.get()), prefix: left_prefix},
                meta_data: tree.meta_data.clone()
            });
            let mut right_prefix = prefix.clone();
            right_prefix.push(false);
            let right = Box::new(NodeTree {
                node: Node::KBuckets{node_entries: right_buckets, timestamp: Cell::new(timestamp.get()), prefix: right_prefix},
                meta_data: tree.meta_data.clone()
            });

            tree.node = Node::Children{left, right};
        }
    }

    fn get_nodes(&self, target: &[u8], num: usize) -> Vec<NodeEntry> {
        NodeTree::get_nodes_at_height(self, target, num, 0)
    }

    pub fn get_bucket_len_nodes(&self, target: &[u8]) -> Vec<NodeEntry> {
        self.get_nodes(target, self.meta_data.bucket_len)
    }

    fn get_nodes_at_height(tree: &NodeTree, target: &[u8], num: usize, height: usize) -> Vec<NodeEntry> {
        match &tree.node {
            Node::Children {left, right} => {
                let target_bits = bits::bytes_to_bits(target);
                let mut left_targets = Vec::new();
                let mut right_targets = Vec::new();
                if *target_bits.get(height).unwrap() {
                    left_targets = NodeTree::get_nodes_at_height(left, target, num, height + 1);
                    if left_targets.len() < num {
                        right_targets = NodeTree::get_nodes_at_height(right, target, num - left_targets.len(), height + 1);
                    }
                } else {
                    right_targets = NodeTree::get_nodes_at_height(right, target, num, height + 1);
                    if right_targets.len() < num {
                        left_targets = NodeTree::get_nodes_at_height(left, target, num - right_targets.len(), height + 1);
                    }
                }

                let mut nodes = Vec::new();
                nodes.extend(left_targets);
                nodes.extend(right_targets);

                nodes
            }
            Node::KBuckets{node_entries, ..} => {
                let mut nodes = node_entries.clone();
                nodes.truncate(num);
                nodes
            }
        }
    }

    #[allow(dead_code)]
    fn get_exact_node_at_height(tree: &NodeTree, target: &[u8], height: usize) -> std::io::Result<NodeEntry> {
        match &tree.node {
            Node::Children {left, right} => {
                let target_bits = bits::bytes_to_bits(target);
                if *target_bits.get(height).unwrap() {
                    NodeTree::get_exact_node_at_height(left, target, height + 1)
                } else {
                    NodeTree::get_exact_node_at_height(right, target, height + 1)
                }
            }
            Node::KBuckets{node_entries, ..} => {
                if let Some(node) = node_entries.iter().find(|&entry|compare_collection::<u8>(&entry.id, target)) {
                    Ok(node.clone())
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "Node not found"))
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn get_node(&self, target: &[u8]) -> std::io::Result<NodeEntry> {
        NodeTree::get_exact_node_at_height(self, target, 0)
    }

#[cfg(test)]
    fn print_tree(&self) {
        match &self.node {
            Node::Children {left, right} => {
                left.print_tree();
                right.print_tree();
            }
            Node::KBuckets{node_entries, prefix, ..} => {
                println!("{:?}, {:?}", prefix.iter().map(|&x|if x { 1 } else { 0 }).collect::<Vec<_>>(), node_entries);
            }
        }
    }

    pub fn put_node(&mut self, node: NodeEntry) -> Result<(), NodeEntry> {
        NodeTree::put_node_at_height(self, node, 0)
    }

    fn remove_node_at_height(tree: &mut NodeTree, target: &[u8], height: usize) {
        match &mut tree.node {
            Node::Children {left, right} => {
                let target_bits = bits::bytes_to_bits(target);
                if *target_bits.get(height).unwrap() {
                    NodeTree::remove_node_at_height(left, target, height + 1)
                } else {
                    NodeTree::remove_node_at_height(right, target, height + 1)
                }
            }
            Node::KBuckets{node_entries, ..} => {
                let mut i = 0;
                while i != node_entries.len() {
                    if compare_collection::<u8>(&node_entries[i].id, target) {
                        let _ = node_entries.remove(i);
                    } else {
                        i += 1;
                    }
                }
            }
        }
    }

    pub fn remove_node(&mut self, target: &[u8]) {
        NodeTree::remove_node_at_height(self, target, 0)
    }

#[cfg(test)]
    pub fn get_all_kbuckets_prefix(&self) -> Vec<Vec<bool>> {
        NodeTree::get_kbuckets_recursive(&self.node, |_|true)
    }

    fn get_kbuckets_recursive<P>(node: &Node, p: P) -> Vec<Vec<bool>>
        where P: Fn(i64) -> bool + Copy {
        let mut result = Vec::new();
        match node {
            Node::Children{left, right} => { result.extend(NodeTree::get_kbuckets_recursive(&left.node, p));
                result.extend(NodeTree::get_kbuckets_recursive(&right.node, p)); }
            Node::KBuckets{timestamp, prefix, ..} => {
                if p(timestamp.get()) {
                    result.push(prefix.to_vec());
                }
            }
        }

        result
    }

#[cfg(test)]
    pub fn get_active_kbuckets_prefix(&self, since: i64) -> Vec<Vec<bool>> {
        NodeTree::get_kbuckets_recursive(&self.node, |timestamp|timestamp > since)
    }

    pub fn get_inactive_kbuckets_prefix(&self, since: i64) -> Vec<Vec<bool>> {
        NodeTree::get_kbuckets_recursive(&self.node, |timestamp|timestamp <= since)
    }
}

#[test]
pub fn test_tree() {
    let bucket_len = 1;
    let node_id = vec![0];
    let mut tree = NodeTree::new(bucket_len, &node_id);

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for i in [0, 2, 4, 8, 16].iter() {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(*i as u8);
        let _ = tree.put_node(node_entry1);
    }

    tree.print_tree();

    let target_id = vec![16];
    let num = 4;
    let result = tree.get_nodes(&target_id, num);
    println!("result = {:?}", result);
    let mut ids:Vec<Vec<u8>> = result.into_iter().map(|x|x.id).collect();
    ids.sort();
    println!("{:?}", ids);
    assert_eq!(ids, [[0], [2], [4], [16]]);
}

#[test]
pub fn test_get_node() {
    let bucket_len = 3;
    let node_id = vec![0, 1, 2, 3, 4, 5];
    let mut tree = NodeTree::new(bucket_len, &node_id);
    let num_node_entries = 150;

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for i in 0..num_node_entries {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(i);
        let _ = tree.put_node(node_entry1);
    }

    for i in 0..bucket_len {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(i as u8);
        let result = tree.get_node(&node_entry1.id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), node_entry1);
    }

    let mut non_existent_node_entry = node_entry.clone();
    non_existent_node_entry.id.push(151);
    let result = tree.get_node(&non_existent_node_entry.id);
    assert!(result.is_err());
}

#[test]
pub fn test_insert_duplicate_node() {
    let bucket_len = 3;
    let node_id = vec![0, 1, 2, 3, 4, 5];
    let mut tree = NodeTree::new(bucket_len, &node_id);
    let num_node_entries = 150;

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for _ in 0..num_node_entries {
        let _ = tree.put_node(node_entry.clone());
    }

    let result = tree.get_node(&node_entry.id);
    assert!(result.is_ok());
}

#[test]
pub fn test_get_all_kbuckets() {
    let bucket_len = 3;
    let node_id = vec![0, 1, 2, 3, 4, 5];
    let mut tree = NodeTree::new(bucket_len, &node_id);
    let num_node_entries = 150;

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for i in 0..num_node_entries {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(i);
        let _ = tree.put_node(node_entry1);
    }

    let result = tree.get_all_kbuckets_prefix();
    println!("result = {:?}", result);
    println!("result.len() = {}", result.len());
    assert!(result.len() > 0 && result.len() <= (node_id.len() + 1)*8);
}

#[test]
pub fn test_get_active_kbuckets() {
    let bucket_len = 3;
    let node_id = vec![0, 1, 2, 3, 4, 5];
    let mut tree = NodeTree::new(bucket_len, &node_id);
    let num_node_entries = 150;

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for i in 0..num_node_entries {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(i);
        let _ = tree.put_node(node_entry1);
    }

    let timestamp = Utc::now().timestamp();

    let sleep_time = time::Duration::from_millis(1000);
    thread::sleep(sleep_time);

    let timestamp1 = Utc::now().timestamp();

    let mut result = tree.get_active_kbuckets_prefix(timestamp1);
    println!("result = {:?}", result);
    println!("result.len() = {}", result.len());
    assert!(result.len() == 0);

    let mut node_entry1 = node_entry.clone();
    node_entry1.id.push(151);
    let mut i = 0;
    while let Err(_) = tree.put_node(node_entry1.clone()) {
        node_entry1.id.pop();
        node_entry1.id.push(i);
        i += 1;
    }

    result = tree.get_active_kbuckets_prefix(timestamp);
    println!("result = {:?}", result);
    println!("result.len() = {}", result.len());
    assert!(result.len() == 1);
}

#[test]
pub fn test_get_inactive_kbuckets() {
    let bucket_len = 3;
    let node_id = vec![0, 1, 2, 3, 4, 5];
    let mut tree = NodeTree::new(bucket_len, &node_id);
    let num_node_entries = 150;

    let node_entry = NodeEntry { addr: vec![1,2,3], port: 333, id: vec![] };

    for i in 0..num_node_entries {
        let mut node_entry1 = node_entry.clone();
        node_entry1.id.push(i);
        let _ = tree.put_node(node_entry1);
    }

    let timestamp = Utc::now().timestamp();

    let sleep_time = time::Duration::from_millis(1000);
    thread::sleep(sleep_time);

    let timestamp1 = Utc::now().timestamp();

    let mut result = tree.get_inactive_kbuckets_prefix(timestamp1);
    println!("result = {:?}", result);
    println!("result.len() = {}", result.len());
    assert!(result.len() <= (node_id.len() + 1)*8);
    let num_kuckets = result.len();

    let mut node_entry1 = node_entry.clone();
    node_entry1.id.push(151);
    let mut i = 0;
    while let Err(_) = tree.put_node(node_entry1.clone()) {
        node_entry1.id.pop();
        node_entry1.id.push(i);
        i += 1;
    }
    result = tree.get_inactive_kbuckets_prefix(timestamp);
    println!("result = {:?}", result);
    println!("result.len() = {}", result.len());
    assert!(result.len() == num_kuckets - 1);
}

#[test]
pub fn test_id_in_range() {
    let mut prefix = Vec::new();
    let mut node_id = Vec::new();
    assert!(NodeTree::id_in_range(&prefix, &node_id));

    prefix = vec!(true);
    node_id.push(0b00000000);
    assert!(!NodeTree::id_in_range(&prefix, &node_id));

    prefix = vec!(false, false);
    assert!(NodeTree::id_in_range(&prefix, &node_id));

    prefix = vec!(false, true);
    assert!(!NodeTree::id_in_range(&prefix, &node_id));
}

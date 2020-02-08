extern crate rand;

use std::iter;
use self::rand::Rng;

pub fn byte_to_bits(input: u8) -> Vec<bool> {
    let mut v = Vec::with_capacity(8);

    for i in 0..8 {
        v.push((input >> i) & 1 != 0);
    }

    v.reverse();

    v
}

/*
 * If input size is not multiple of eight, pad 0 at low bits:
 * Input: [true, true, false, true]
 * Output: 0b10010000
 */
pub fn bits_to_byte(bits: &[bool]) -> u8 {
    let mut bits_padded = bits.to_vec();

    if bits.len() % 8 != 0 {
        let padding_num = 8 - bits.len() % 8;
        bits_padded.extend(iter::repeat(false).take(padding_num));
    }

    let mut result = 0;

    for b in bits_padded {
        if b {
            result = 1 | (result << 1);
        } else {
            result <<= 1;
        }
    }

    result
}

pub fn bits_to_bytes(bits: &[bool]) -> Vec<u8> {
    bits.chunks(8).map(|v|bits_to_byte(v)).collect()
}

pub fn bytes_to_bits(input: &[u8]) -> Vec<bool> {
    let mut v = Vec::new();

    for b in input {
        v.extend_from_slice(&byte_to_bits(*b));
    }

    v
}

pub fn get_distance(v1: &[u8], v2: &[u8]) -> Vec<u8> {
    v1
    .iter()
    .zip(v2.iter())
    .map(|(&x1, &x2)| x1 ^ x2)
    .collect()
}

pub fn gen_random_id_in_bucket(prefix: &[bool], id_len: usize) -> Vec<u8> {
    assert!(prefix.len() <= id_len * 8);

    let pad_num = id_len * 8 - prefix.len();

    let mut bits_padded = prefix.to_vec();
    if pad_num != 0 {
        let rand_bits: Vec<bool> = rand::thread_rng().sample_iter(&rand::distributions::Standard).take(pad_num).collect();
        bits_padded.extend(rand_bits);
    }

    bits_to_bytes(&bits_padded)
}

#[test]
fn test_byte_to_bits() {
    let b = 0xFF;
    let b_zero = 0x00;
    let b_one = 0x01;

    assert_eq!(byte_to_bits(b), [true; 8].to_vec());
    assert_eq!(byte_to_bits(b_zero), [false; 8].to_vec());

    let mut b_one_result = [false; 7].to_vec();
    b_one_result.push(true);
    assert_eq!(byte_to_bits(b_one), b_one_result);
}

#[test]
fn test_bytes_to_bits() {
    let v = [0xFF, 0x00, 0xFE];
    let mut result =[true; 8].to_vec();
    result.extend([false; 8].to_vec());
    result.extend([true, true, true, true, true, true, true, false].to_vec());

    assert_eq!(bytes_to_bits(&v), result);
}

#[test]
fn test_get_distance() {
    let v1 = [0xFF, 0x00, 0xFE];
    let v2 = [0; 3];
    assert!(get_distance(&v1, &v2) == v1.to_vec());
    assert!(get_distance(&v1, &v1) == v2.to_vec());

    let v3 = [0xFE, 0x00, 0x0E];
    let v4 = [0x01, 0x00, 0xF0];
    assert!(get_distance(&v1, &v3) == v4.to_vec());
}

#[test]
fn test_bits_to_bytes() {
    let v1 = vec!(true, false, true, false);
    let mut result = bits_to_bytes(&v1);

    assert!(result == vec!(0b10100000));

    let v2 = vec!(true, true, true, true, true, false, true, false,
                  true);
    result = bits_to_bytes(&v2);
    assert!(result == vec!(0b11111010, 0b10000000));
}

#[test]
fn test_gen_random_id_in_bucket() {
    let prefix = vec!(true, false, true, false);
    let id_len = 8;
    let id = gen_random_id_in_bucket(&prefix, id_len);
    assert!(id.len() == id_len);
    let bits = bytes_to_bits(&id);
    assert!(prefix == bits.iter().map(|&x|x).take(prefix.len()).collect::<Vec<_>>());

    let id2 = gen_random_id_in_bucket(&prefix, id_len);
    assert!(id != id2); // assume two consectively generated random ids will no be equal
}

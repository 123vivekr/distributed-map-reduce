mod worker_node;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use worker_node::make_worker;
use mr::KVPair;

fn main() {
    let mut args = env::args().skip(1);

    let coordinator_ip = match args.next() {
        Some(ip) => ip,
        None => panic!("Coordinator IP empty"),
    };

    let n_workers = match args.next() {
        Some(n) => n,
        None => panic!("Number of workers empty"),
    };

    let n_workers = n_workers.parse().expect("Number of workers should be a number");

    let mut handles = Vec::new();
    for _ in 0..n_workers {
        let coordinator_ip = coordinator_ip.clone();
        handles.push(thread::spawn(move || {
            make_worker(coordinator_ip, map, reduce);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn map(_filename: String, content: String) -> Vec<KVPair> {
    let is_word = |&x: &&str| !x.is_empty();
    let mut kva: Vec<KVPair> = Vec::new();

    let words = content.split(&[' ', '\n'][..]).filter(is_word);
    for word in words {
        let x: &[_] = &[',', '.'];
        let cleaned_word = word.trim_matches(x);
        kva.push(KVPair {
            key: cleaned_word.to_ascii_lowercase().to_string(),
            value: "1".to_string(),
        });
    }
    kva
}

fn reduce(_key: String, values: Vec<String>) -> String {
    let result = String::from(format!("{}", values.len()));
    result
}
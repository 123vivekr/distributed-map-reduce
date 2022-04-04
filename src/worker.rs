mod worker_node;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use worker_node::make_worker;
use mr::KVPair;

fn main() {
    let coordinator_ip = match env::args().skip(1).next() {
        Some(ip) => ip,
        None => panic!("Coordinator IP empty"),
    };

    // get map reduce functions
    // let map: fn(String, String) -> Vec<KVPair> = ;
    // let reduce: fn(String, Vec<String>) -> String = ;

    // make_worker(mapf, reducef);
    // let mut handles = Vec::new();

    make_worker(coordinator_ip, map, reduce);

    // for i in 0..10 {
    //     let coordinator_ip = coordinator_ip.clone();
    //     handles.push(thread::spawn(move || {
    //     }));
    // }

    // for handle in handles {
    //     handle.join().unwrap();
    // }

    // dispatch map reduce to worker
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
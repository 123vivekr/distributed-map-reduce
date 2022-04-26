use std::env;
mod coordinator_node;
use coordinator_node::make_coordinator;

fn main() {
    let mut args = env::args().skip(1);

    let n_reduce = match args.next() {
        Some(n) => n,
        None => panic!("Number of workers empty"),
    };
    let n_reduce = n_reduce.parse().expect("NReduce should be a number");

    let filenames: Vec<String> = args.collect();
    println!("{:?}", filenames);
    make_coordinator(filenames, n_reduce);
}

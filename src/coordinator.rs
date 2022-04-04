use std::env;
mod coordinator_node;
use coordinator_node::make_coordinator;

fn main() {
    let files: Vec<String> = env::args().skip(1).collect();
    println!("{:?}", files);
    make_coordinator(files, 5);
}

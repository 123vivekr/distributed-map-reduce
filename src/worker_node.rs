use mr::{FromServer, FromWorker, KVPair, Operation, Task};
use serde_json;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::{self, Hash, Hasher};
use std::io::BufReader;
use std::io::Read;
use std::io::{BufRead, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

struct Worker {
    coordinator_ip: String,
    filename: Option<String>,
    mapf: fn(String, String) -> Vec<KVPair>,
    reducef: fn(String, Vec<String>) -> String,
    n_reduce: u8,
}

impl Worker {
    fn stub(&self, payload: FromWorker) -> FromServer {
        let mut stream = TcpStream::connect(&self.coordinator_ip).unwrap();

        let payload = serde_json::to_vec(&payload).unwrap();
        stream.write(&payload).unwrap();

        let mut buf = [0; 1024];
        let size = stream.read(&mut buf).unwrap();
        let s = String::from_utf8_lossy(&buf);
        println!("{}", s);
        serde_json::from_slice::<FromServer>(&buf[..size]).unwrap()
    }

    pub fn new(
        coordinator_ip: String,
        mapf: fn(String, String) -> Vec<KVPair>,
        reducef: fn(String, Vec<String>) -> String,
    ) -> Worker {
        Worker {
            coordinator_ip,
            filename: Option::None,
            mapf,
            reducef,
            n_reduce: 0,
        }
    }

    pub fn start(&mut self) {
        if let FromServer::NReduce(n_reduce) = self.stub(FromWorker::GetNReduce) {
            self.n_reduce = n_reduce;
        }

        loop {
            if let FromServer::Task(task) = self.stub(FromWorker::Fetch) {
                self.start_executor(task);
            } else {
                thread::sleep(Duration::from_secs(5));
            }
        }
    }

    fn start_executor(&self, task: Task) {
        match task {
            Task::Map(filename) => {
                let mut file = File::open(&filename).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();
                let mut kva = (self.mapf)(filename.clone(), contents);
                let mut file_number;
                let mut file_map = HashMap::new();

                for n in 1..self.n_reduce + 1 {
                    file_map.insert(
                        n as u64,
                        File::create(format!("intermediate-{}-{}", n, filename)).unwrap(),
                    );
                }

                for kv_pair in kva {
                    let mut s = DefaultHasher::new();
                    kv_pair.key.hash(&mut s);
                    file_number = s.finish() % self.n_reduce as u64;

                    if let Some(file) = file_map.get(&file_number) {
                        serde_json::to_writer(file, &kv_pair).unwrap();
                    }
                }

                println!("File {} done", filename);
            }
            Task::Reduce(reduce_number) => {
                let mut kvm: HashMap<String, Vec<String>> = HashMap::new();
                for path in fs::read_dir("./").unwrap() {
                    let file_name = path.unwrap().file_name().into_string().unwrap();
                    if file_name.contains(("intermediate-")) {
                        // split file_name with '-' as delimiter.
                        let mut file_name_vec: Vec<&str> = file_name.split('-').collect();
                        if file_name_vec[1].parse::<u8>().unwrap() == reduce_number {
                                let file = File::open(file_name).unwrap();
                                let mut deserializer = serde_json::Deserializer::from_reader(file)
                                    .into_iter::<KVPair>();

                                for pair_result in deserializer {
                                    if let Ok(pair) = pair_result {
                                        if let Some(values) = kvm.get_mut(&pair.key) {
                                            values.push(pair.value);
                                        } else {
                                            kvm.insert(pair.key, vec![pair.value]);
                                        }
                                    }
                                }
                        }
                    }
                }

                let reduce_file = File::create(format!("mr-out-{}", reduce_number)).unwrap();
                for (key, value_array) in kvm.iter() {
                    let result_value = (self.reducef)(key.clone(), value_array.clone());
                    let kv_pair = KVPair {
                        key: key.clone(),
                        value: result_value,
                    };
                    serde_json::to_writer(&reduce_file, &kv_pair).unwrap();
                }
            }
        }
    }
}

pub fn make_worker(
    coordinator_ip: String,
    mapf: fn(String, String) -> Vec<KVPair>,
    reducef: fn(String, Vec<String>) -> String,
) {
    let mut worker = Worker::new(coordinator_ip, mapf, reducef);
    worker.start();

    // match payload {
    //     FromServer::PingResponse => {
    //         println!("Server responded to ping!");
    //     },
    //     FromServer::Task { operation, filename } => {
    //         match operation {
    //             Operation::Map => mapf(filename, content),
    //             Operation::Reduce => ,
    //         }
    //     },
    //     FromServer::Wait => {
    //         println!("Server told to wait");
    //     },
    // };
}

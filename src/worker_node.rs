use mr::{FromServer, FromWorker, KVPair, Task};
use serde_json;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

struct Worker {
    coordinator_ip: String,
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
        serde_json::from_slice::<FromServer>(&buf[..size]).unwrap()
    }

    pub fn new(
        coordinator_ip: String,
        mapf: fn(String, String) -> Vec<KVPair>,
        reducef: fn(String, Vec<String>) -> String,
    ) -> Worker {
        let mut worker = Worker {
            coordinator_ip,
            mapf,
            reducef,
            // temporarily assign 0 to n_reduce for creating worker object
            n_reduce: 0,
        };

        // get n_reduce from coordinator
        if let FromServer::NReduce(n_reduce) = worker.stub(FromWorker::GetNReduce) {
            worker.n_reduce = n_reduce;
        } else {
            panic!("Error getting n_reduce from coordinator");
        }

        worker
    }

    pub fn start(&mut self) {
        let worker_wait_timeout = Duration::from_secs(5);

        loop {
            // get task for map or reduce from server
            if let FromServer::Task(task) = self.stub(FromWorker::Fetch) {
                self.start_executor(task);
            } else {
                // wait if no task received from server
                thread::sleep(worker_wait_timeout);
            }
        }
    }

    fn start_executor(&self, task: Task) {
        match task {
            Task::Map(filename) => {
                println!("{}", filename);

                // get file contents
                let mut file = File::open(&filename).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();

                // run map function over file contents
                let key_value_vec = (self.mapf)(filename.clone(), contents);

                // create n_reduce + 1 "intermediate-filename" files
                let mut file_number;
                let mut file_map = HashMap::new();
                for n in 1..self.n_reduce + 1 {
                    file_map.insert(
                        n as u64,
                        File::create(format!("intermediate-{}-{}", n, filename)).unwrap(),
                    );
                }

                // shuffle map output using hash and write to n_reduce files
                for kv_pair in key_value_vec {
                    let mut s = DefaultHasher::new();
                    kv_pair.key.hash(&mut s);
                    file_number = s.finish() % self.n_reduce as u64 + 1;

                    if let Some(file) = file_map.get(&file_number) {
                        serde_json::to_writer(file, &kv_pair).unwrap();
                    }
                }
            }
            Task::Reduce(reduce_number) => {
                let mut key_vector_map: HashMap<String, Vec<String>> = HashMap::new();
                for path in fs::read_dir("./").unwrap() {
                    let file_name = path.unwrap().file_name().into_string().unwrap();
                    if file_name.contains("intermediate-") {

                        // split file_name with '-' as delimiter.
                        let file_name_vec: Vec<&str> = file_name.split('-').collect();

                        // file_name_vec[1] has the reduce_number; for each intermediate file
                        // example: intermedite-reduce_number-filename.txt
                        if file_name_vec[1].parse::<u8>().unwrap() == reduce_number {
                            let file = File::open(file_name).unwrap();

                            // deserialize file into Vec<KVPair>
                            let deserializer =
                                serde_json::Deserializer::from_reader(file).into_iter::<KVPair>();
                            
                            // create map with key:array
                            // array with each value elements for the same key
                            for pair_result in deserializer {
                                if let Ok(pair) = pair_result {
                                    if let Some(values) = key_vector_map.get_mut(&pair.key) {
                                        values.push(pair.value);
                                    } else {
                                        key_vector_map.insert(pair.key, vec![pair.value]);
                                    }
                                }
                            }
                        }
                    }
                }

                let reduce_file = File::create(format!("mr-out-{}", reduce_number)).unwrap();
                for (key, value_array) in key_vector_map.iter() {

                    // run reduce function on value array for each key
                    let result_value = (self.reducef)(key.clone(), value_array.clone());
                    let kv_pair = KVPair {
                        key: key.clone(),
                        value: result_value,
                    };

                    // write to file
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
}

use mr::{FromServer, FromWorker, Operation, Task};
use spmc;
use std::collections::HashMap;
use std::fs;
use std::hash::Hash;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::RangeBounds;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct Process {
    worker_socket_addr: SocketAddr,
    filename: Option<String>,
    timestamp: Instant,
}

pub struct Coordinator {
    filenames: Vec<String>,
    operation: Arc<RwLock<Operation>>,
    n_reduce: u8,
}

impl Coordinator {
    fn new(filenames: Vec<String>, n_reduce: u8) -> Coordinator {
        Coordinator {
            filenames: filenames,
            operation: Arc::new(RwLock::new(Operation::Map)),
            n_reduce,
        }
    }

    pub fn start(&self) {
        // create channels for server - task managaer communication
        let (pending_task_tx, pending_task_rx) = spmc::channel();
        let (processing_task_tx, processing_task_rx) = mpsc::channel();

        // start task manager thread
        self.start_task_manager(pending_task_tx, processing_task_rx);

        // start server thread
        self.start_server(pending_task_rx, processing_task_tx);
    }

    fn start_task_manager(
        &self,
        mut pending_task_tx: spmc::Sender<String>,
        processing_task_rx: mpsc::Receiver<Process>,
    ) {
        // load filenames into pending task channel
        for filename in &self.filenames {
            pending_task_tx.send(filename.clone()).unwrap();
        }

        // start another thread to check for dead processing tasks
        // if dead tasks found, mvoe task to pending task channel
        let mut worker_process_table: HashMap<SocketAddr, Process> = HashMap::new();

        let total_files = self.filenames.len();
        let operation_rwlock_clone = self.operation.clone();
        let n_reduce_clone = self.n_reduce;
        let timeout_threshold = Duration::from_secs(5);

        thread::spawn(move || {
            loop {
                // on worker ping, create or update process
                while let Ok(new_process) = processing_task_rx.try_recv() {
                    if new_process.filename == Option::None {
                        let worker_socket_addr = new_process.worker_socket_addr;
                        if let Some(old_process) = worker_process_table.get_mut(&worker_socket_addr)
                        {
                            old_process.timestamp = new_process.timestamp;
                        }
                    } else {
                        worker_process_table.insert(new_process.worker_socket_addr, new_process);
                    }
                }

                // wait 5 second
                thread::sleep(Duration::from_secs(5));

                if *operation_rwlock_clone.read().unwrap() == Operation::Map {
                    // get all intermediate filenames from directory
                    let mut intermediate_file_map: HashMap<String, u8> = HashMap::new();
                    for path in fs::read_dir("./").unwrap() {
                        let file_name = path.unwrap().file_name().into_string().unwrap();
                        if file_name.contains("intermediate-") {
                            // split file_name with '-' as delimiter.
                            // last part is the original filename
                            if let Some(file_name) =
                                file_name.split('-').collect::<Vec<&str>>().pop()
                            {
                                if let Some(count) = intermediate_file_map.get_mut(file_name)
                                {
                                    *count += 1;
                                } else {
                                    intermediate_file_map.insert(file_name.to_string(), 1);
                                }
                            }
                        }
                    }

                    // get all completed files so that worker data could be deleted
                    let mut finished_file_count = 0;
                    let mut finished_worker_map = HashMap::new();
                    for (filename, &count) in intermediate_file_map.iter() {
                        if count == n_reduce_clone {
                            finished_worker_map.insert(filename.to_string(), true);
                            finished_file_count += 1;
                        }
                    }

                    // get timed out workers
                    // check if each process is still alive
                    // just check if process.timestamp older than 5 seconds
                    // if older, send process.filename into pending_task channel
                    let mut timed_out_worker_list = Vec::new();
                    for (worker_socket_addr, process) in worker_process_table.iter() {
                        if process.timestamp + timeout_threshold > Instant::now() {
                            timed_out_worker_list.push(worker_socket_addr.clone());
                        }
                    }

                    for worker_socket_addr in timed_out_worker_list {
                        if let Some(process) = worker_process_table.remove(&worker_socket_addr) {
                            if let Some(filename) = &process.filename {
                                // only send to pending task chanenel if work is not finished
                                if finished_worker_map.get(filename) == None {
                                    pending_task_tx.send(filename.clone()).unwrap();
                                }
                            };
                        }
                    }

                    // if all files have gone through map phase, switch to Reduce
                    if finished_file_count == total_files {
                        println!("Switching to Reduce");
                        *operation_rwlock_clone.write().unwrap() = Operation::Reduce;
                        for n in 1..n_reduce_clone+1 {
                            pending_task_tx.send(format!("{}", n)).unwrap();
                        }
                    }
                } else {
                    // find out what all reduce operations are completed based on filenames
                    // example: mr-out-<reduce_task_number>
                    let mut reduce_finished_worker_map: HashMap<String, u8> = HashMap::new();
                    for path in fs::read_dir("./").unwrap() {
                        let file_name = path.unwrap().file_name().into_string().unwrap();
                        if file_name.contains("mr-out-") {
                            // split file_name with '-' as delimiter.
                            // last part is the reduce task number
                            if let Some(file_name) =
                                file_name.split('-').collect::<Vec<&str>>().pop()
                            {
                                if let Some(count) = reduce_finished_worker_map.get_mut(file_name)
                                {
                                    *count += 1;
                                } else {
                                    reduce_finished_worker_map.insert(file_name.to_string(), 1);
                                }
                            }
                        }
                    }

                    // get timed out workers
                    let mut timed_out_worker_list = Vec::new();
                    for (worker_socket_addr, process) in worker_process_table.iter() {
                        if process.timestamp + timeout_threshold > Instant::now() {
                            timed_out_worker_list.push(worker_socket_addr.clone());
                        }
                    } 

                    // delete workers and reissue operation only for timed out workers
                    for worker_socket_addr in timed_out_worker_list {
                        if let Some(process) = worker_process_table.remove(&worker_socket_addr) {
                            if let Some(filename) = &process.filename {
                                if reduce_finished_worker_map.get(filename) == None {
                                    pending_task_tx.send(filename.clone()).unwrap();
                                }
                            };
                        }
                    }
                }
            }
        });
    }

    // handles worker ping
    // issues tasks to worker
    fn start_server(
        &self,
        pending_task_rx: spmc::Receiver<String>,
        processing_task_tx: mpsc::Sender<Process>,
    ) {
        let operation_rwlock_clone = self.operation.clone();
        let n_reduce = Arc::new(self.n_reduce);

        thread::spawn(move || {
            // TODO: let addr = "[::1]:50051".parse().unwrap();
            let listener = TcpListener::bind("127.0.0.1:12345").unwrap();
            println!("Listening on localhost:12345");


            for stream in listener.incoming() {
                let stream = stream.unwrap();

                // clone channel ends for each handler thread
                let pending_task_rx_handle = pending_task_rx.clone();
                let processing_task_tx_handle = processing_task_tx.clone();

                let operation_rwlock_clone = operation_rwlock_clone.clone();
                let n_reduce_clone = n_reduce.clone();

                thread::spawn(move || {
                    handle_worker(
                        stream,
                        pending_task_rx_handle,
                        processing_task_tx_handle,
                        operation_rwlock_clone,
                        n_reduce_clone,
                    );
                });
            }
        })
        .join()
        .unwrap();
    }
}

// the logic behind server which handles a worker
fn handle_worker(
    mut stream: TcpStream,
    pending_task_rx: spmc::Receiver<String>,
    mut processing_task_tx: mpsc::Sender<Process>,
    operation_rwlock_clone: Arc<RwLock<Operation>>,
    n_reduce_clone: Arc<u8>,
) {
    let mut buf = [0; 1024];
    let size = stream.read(&mut buf).unwrap();
    let payload: FromWorker = serde_json::from_slice(&buf[..size]).unwrap();

    let response_payload: FromServer;

    match payload {
        FromWorker::Ping => {
            let process = Process {
                worker_socket_addr: stream.peer_addr().unwrap(),
                filename: Option::None, // this is used for pings
                timestamp: Instant::now(),
            };

            processing_task_tx.send(process).unwrap();
            response_payload = FromServer::PingResponse;
        }
        FromWorker::Fetch => {
            if let Ok(filename) = pending_task_rx.try_recv() {
                let process = Process {
                    worker_socket_addr: stream.peer_addr().unwrap(),
                    filename: Some(filename.clone()),
                    timestamp: Instant::now(),
                };

                processing_task_tx.send(process).unwrap();
                if *operation_rwlock_clone.read().unwrap() == Operation::Map {
                    response_payload = FromServer::Task(Task::Map(filename));
                } else {
                    response_payload = FromServer::Task(Task::Reduce(filename.parse().unwrap()));
                }
            } else {
                // no task now. wait and retry later
                response_payload = FromServer::Wait;
            }
        },
        FromWorker::GetNReduce => {
            response_payload = FromServer::NReduce(*n_reduce_clone);
        }
    };

    let json_payload = serde_json::to_vec(&response_payload).unwrap();
    stream.write(&json_payload).unwrap();
}

pub fn make_coordinator(filenames: Vec<String>, n_reduce: u8) {
    let coordinator = Coordinator::new(filenames, n_reduce);
    coordinator.start();
}
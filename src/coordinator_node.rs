use mr::{FromServer, FromWorker, Task};
use spmc;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TIMEOUT_THRESHOLD: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Map,
    Reduce,
}

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
            filenames,
            operation: Arc::new(RwLock::new(Operation::Map)),
            n_reduce,
        }
    }

    /// Start coordinator processes
    /// This will run a server which handles worker node requests
    /// and a task manager to keep track of pending and completed tasks
    #[tokio::main]
    pub async fn start(&self) {
        // create channels for server - task manager communication
        let (pending_task_tx, pending_task_rx) = spmc::channel();
        let (processing_task_tx, processing_task_rx) = mpsc::channel();

        // TODO: Here task man and server start on 2 different threads; can we get it to run asynchronously?
        // Perhaps using a select() operation?

        // start task manager thread
        self.start_task_manager(pending_task_tx, processing_task_rx);

        // start server thread
        self.start_server(pending_task_rx, processing_task_tx).await;
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

        // keeps track of socket addresss of workers and the process they are running
        let mut worker_process_table: HashMap<SocketAddr, Process> = HashMap::new();

        // worker ping timeout threshold
        let total_files = self.filenames.len();
        let operation_rwlock_clone = self.operation.clone();
        let n_reduce_clone = self.n_reduce;

        thread::spawn(move || {
            loop {
                // on worker ping, create process or update process timestamp
                // if worker sends in Option::None for filename then it is just a ping
                // so just update the timestamp for the process
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
                #[cfg(not(test))]
                thread::sleep(Duration::from_secs(5));

                if *operation_rwlock_clone.read().unwrap() == Operation::Map {
                    // get all intermediate filenames from directory
                    // each filename will have n_reduce intermediate files
                    // TODO: no need to initialize every iteration
                    let mut intermediate_file_map: HashMap<String, u8> = HashMap::new();
                    for path in fs::read_dir("./").unwrap() {
                        let file_name = path.unwrap().file_name().into_string().unwrap();
                        if file_name.contains("intermediate-") {
                            // split file_name with '-' as delimiter.
                            if let Some(file_name) =
                                // example full filename: intermediate-n-filename.txt
                                // so, get only the last part which is the actual filename
                                file_name.split('-').collect::<Vec<&str>>().pop()
                            {
                                if let Some(count) = intermediate_file_map.get_mut(file_name) {
                                    *count += 1;
                                } else {
                                    intermediate_file_map.insert(file_name.to_string(), 1);
                                }
                            }
                        }
                    }

                    // get all completed files so that worker data could be deleted
                    // go though intermediate_file map and check how many intermediate
                    // files there are for each filename and if it matches n_reduce, then
                    // the map process is completed for that filename
                    let mut finished_file_count = 0;
                    // TODO: no need to initialize every iteration
                    let mut finished_worker_map = HashMap::new();
                    for (filename, &count) in intermediate_file_map.iter() {
                        if count == n_reduce_clone {
                            finished_worker_map.insert(filename.to_string(), true);
                            finished_file_count += 1;
                        }
                    }

                    // get timed out workers
                    // check if each process is still alive
                    // just check if process.timestamp older than timeout_threshold
                    // if older, send process.filename into pending_task channel
                    let mut timed_out_worker_list = Vec::new();
                    for (worker_socket_addr, process) in worker_process_table.iter() {
                        if process.timestamp + TIMEOUT_THRESHOLD < Instant::now() {
                            timed_out_worker_list.push(worker_socket_addr.clone());
                        }
                    }

                    // delete timed out workers from worker_process table
                    // re issue task to pending channel if task not complete
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
                        for n in 1..n_reduce_clone + 1 {
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
                                if let Some(count) = reduce_finished_worker_map.get_mut(file_name) {
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
                        if process.timestamp + TIMEOUT_THRESHOLD < Instant::now() {
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
    async fn start_server(
        &self,
        pending_task_rx: spmc::Receiver<String>,
        processing_task_tx: mpsc::Sender<Process>,
    ) {
        let operation_rwlock_clone = self.operation.clone();
        let n_reduce = Arc::new(self.n_reduce);

        // TODO: enable option for arbitary IP
        // let addr = "[::1]:50051".parse().unwrap();
        let listener = TcpListener::bind("127.0.0.1:12345").await.unwrap();
        println!("Listening on localhost:12345");

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            let pending_task_rx_handle = pending_task_rx.clone();
            let processing_task_tx_handle = processing_task_tx.clone();

            let operation_rwlock_clone = operation_rwlock_clone.clone();
            let n_reduce_clone = n_reduce.clone();

            tokio::spawn(async move {
                handle_worker(
                    stream,
                    pending_task_rx_handle,
                    processing_task_tx_handle,
                    operation_rwlock_clone,
                    n_reduce_clone,
                )
                .await;
            });
        }
    }
}

// the logic behind server which handles a worker
async fn handle_worker(
    mut stream: TcpStream,
    pending_task_rx: spmc::Receiver<String>,
    processing_task_tx: mpsc::Sender<Process>,
    operation_rwlock_clone: Arc<RwLock<Operation>>,
    n_reduce_clone: Arc<u8>,
) {
    let mut buf = [0; 1024];
    let size = stream.read(&mut buf).await.unwrap();
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
        }
        FromWorker::GetNReduce => {
            response_payload = FromServer::NReduce(*n_reduce_clone);
        }
    };

    let json_payload = serde_json::to_vec(&response_payload).unwrap();
    stream.write(&json_payload).await.unwrap();
}

/// Create a coordinator node
/// `filenames` is a list of all the input filenames
/// `n_reduce` is the number of reduce tasks
pub fn make_coordinator(filenames: Vec<String>, n_reduce: u8) {
    let coordinator = Coordinator::new(filenames, n_reduce);
    coordinator.start();
}

#[cfg(test)]
mod tests {
    use super::*;

    // Task Manager tests
    #[test]
    fn load_filenames_into_pending_task_channel() {
        let filenames = vec![String::from("file1.txt"), String::from("file2.txt")];
        let coordinator = Coordinator::new(filenames, 1);
        let (pending_task_tx, pending_task_rx) = spmc::channel();
        let (_, processing_task_rx) = mpsc::channel();

        coordinator.start_task_manager(pending_task_tx, processing_task_rx);

        let file1 = pending_task_rx.recv().unwrap();
        assert_eq!(file1, String::from("file1.txt"));
        let file2 = pending_task_rx.recv().unwrap();
        assert_eq!(file2, String::from("file2.txt"));
        let empty = pending_task_rx.try_recv();
        assert_eq!(empty, Err(spmc::TryRecvError::Empty));
    }

    #[test]
    fn switch_to_reduce_mode_after_all_map_tasks_complete() {
        let filenames = vec![String::from("file1.txt"), String::from("file2.txt")];
        let coordinator = Coordinator::new(filenames, 1);
        let (pending_task_tx, pending_task_rx) = spmc::channel();
        let (_, processing_task_rx) = mpsc::channel();
        let operation_rwlock_clone = coordinator.operation.clone();
        assert_eq!(*operation_rwlock_clone.read().unwrap(), Operation::Map);
        std::fs::File::create("intermediate-1-file1.txt").unwrap();
        std::fs::File::create("intermediate-1-file2.txt").unwrap();

        coordinator.start_task_manager(pending_task_tx, processing_task_rx);

        let file1 = pending_task_rx.recv().unwrap();
        assert_eq!(file1, String::from("file1.txt"));
        let file2 = pending_task_rx.recv().unwrap();
        assert_eq!(file2, String::from("file2.txt"));
        let one = pending_task_rx.recv();
        assert_eq!(one, Ok(String::from("1")));
        assert_eq!(*operation_rwlock_clone.read().unwrap(), Operation::Reduce);
        std::fs::remove_file("intermediate-1-file1.txt").unwrap();
        std::fs::remove_file("intermediate-1-file2.txt").unwrap();
    }

    #[test]
    fn handle_timed_out_worker() {
        let filename = vec![String::from("file1.txt")];
        let sample_worker_socket_addr = "127.0.0.1:8080".parse().unwrap();
        let coordinator = Coordinator::new(filename, 1);
        let (pending_task_tx, pending_task_rx) = spmc::channel();
        let (processing_task_tx, processing_task_rx) = mpsc::channel();

        coordinator.start_task_manager(pending_task_tx, processing_task_rx);
        // get filename form pending_task_channel
        if let Ok(filename) = pending_task_rx.try_recv() {
            // ensure pending_task_channel empty
            let empty = pending_task_rx.try_recv();
            assert_eq!(empty, Err(spmc::TryRecvError::Empty));

            // put process on processing_task channel
            let process = Process {
                worker_socket_addr: sample_worker_socket_addr,
                filename: Some(filename.clone()),
                timestamp: Instant::now() - TIMEOUT_THRESHOLD, // timed out
            };
            processing_task_tx.send(process).unwrap();
        }

        let file1 = pending_task_rx.recv().unwrap();
        assert_eq!(file1, String::from("file1.txt"));
    }

    // Server tests
    // #[tokio::test]
    // async fn ping_should_update_timestamp() {
    //     let filename = vec![String::from("file1.txt")];
    //     let (mut pending_task_tx, pending_task_rx) = spmc::channel();
    //     let (processing_task_tx, processing_task_rx) = mpsc::channel();
    //     let worker_socket_addr: SocketAddr = "127.0.0.2:12345".parse().unwrap();

    //     // put task on pending_task channel
    //     pending_task_tx.send(String::from("file1.txt")).unwrap();

    //     // get task from server; check if task is on processing_task_tx
    //     #[double]
    //     use tokio::net::TcpStream;

    //     handle_worker(stream, pending_task_rx, processing_task_tx, Arc::new(RwLock::new(Operation::Map)), Arc::new(1));


    //     // ping server after a delay; check if server respond with ping
    //     // check processing_task_tx channel for updated timestamp
    // }

    // #[tokio::test]
    // async fn fetch_should_return_valid_map_or_reduce_task() {
    //     let filenames = vec![String::from("file1.txt")];
    //     let coordinator = Coordinator::new(filenames, 1);
    //     let (pending_task_tx, pending_task_rx) = spmc::channel();
    //     let (processing_task_tx, processing_task_rx) = mpsc::channel();
    //     let worker_addr: SocketAddr = "127.0.0.2:12345".parse().unwrap();

    //     coordinator.start_server(pending_task_rx, processing_task_tx).await;

    //     // put map task on pending_task channel
    //     // fetch task from server
    //     // check if task is valid map

    //     // put reduce task on pending_task channel
    //     // fetch task from server
    //     // check if task is valid reduce
    // }
}

// sytax = "proto3";

// package rpc;

// message Null {}

// message WorkerID {
//     string IP = 1;
// }

// message Work {
//     string filename = 1;
//     string operation = 2;
// }

// service Rpc {
//     rpc RequestWork (WorkerID) returns (Work);
// }
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum FromWorker {
    Ping,
    Fetch,
    GetNReduce,
}

#[derive(Deserialize, Serialize)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum FromServer {
    PingResponse,
    Task(Task),
    Wait,
    NReduce(u8),
}


#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Task {
    Map(String),
    Reduce(u8),
}
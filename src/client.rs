//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::IpcSender as Sender;
use client::ipc_channel::ipc::TryRecvError;

use message;
use message::MessageType;
use message::RequestStatus;

use crate::message::ProtocolMessage;
use crate::recieve_and_check_for_finish;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {
    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(
        id_str: String,
        running: Arc<AtomicBool>,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Client {
        Client {
            id_str: id_str,
            running: running,
            num_requests: 0,
            tx: tx,
            rx: rx,
            failed_ops: 0,
            successful_ops: 0,
            unknown_ops: 0, // TODO
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // println!("Client is done, waiting on the coordinator");
        if self.running.load(Ordering::SeqCst) {
            let exit_msg = self.recv_result();

            if exit_msg != MessageType::CoordinatorExit {
                panic!(
                    "Recieved a non exit messae from the coordinator {:?}",
                    exit_msg
                );
            }
        }

        // TODO

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {
        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        let pm = message::ProtocolMessage::generate(
            message::MessageType::ClientRequest,
            txid.clone(),
            self.id_str.clone(),
            self.num_requests,
        );
        info!(
            "{}::Sending operation #{}",
            self.id_str.clone(),
            self.num_requests
        );

        // TODO
        self.tx.send(pm).unwrap();
        // println!("sent op for client");

        trace!(
            "{}::Sent operation #{}",
            self.id_str.clone(),
            self.num_requests
        );
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) -> MessageType {
        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        let res = match recieve_and_check_for_finish(&self.rx) {
            Some(m) => m,
            None => {
                self.running.store(false, Ordering::SeqCst);
                return MessageType::CoordinatorExit;
            }
        };

        // println!("Recieved result for client");
        return res.mtype;

        // TODO
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats

        println!(
            "{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id_str.clone(),
            self.successful_ops,
            self.failed_ops,
            self.unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {
        // println!("Starting to run protocol for client");

        // TODO
        for i in 0..n_requests {
            if !self.running.load(Ordering::SeqCst) {
                break;
            };
            // println!("Running request{} ", i);
            self.send_next_operation();
            let coord_res = self.recv_result();

            if coord_res == MessageType::ClientResultCommit {
                // println!("Coordinator got client success");
                self.successful_ops += 1;
            } else if coord_res == MessageType::ClientResultAbort {
                // println!("Coordinator got client abort");

                self.failed_ops += 1;
            } else if coord_res == MessageType::CoordinatorExit {
                // println!("Coordinator has disconectted");
                break;
            } else {
                panic!("Invalid message for client: {:?}", coord_res);
            }
        }

        self.wait_for_exit_signal();
        self.report_status();
    }
}

//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::IpcSender as Sender;
use participant::ipc_channel::ipc::TryRecvError;
use participant::rand::prelude::*;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::coordinator::Coordinator;
use crate::{message, recieve_and_check_for_finish};

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {
    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Participant {
        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            rx: rx,
            tx: tx, // TODO
            failed_ops: 0,
            successful_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();

        if x <= self.send_success_prob {
            self.tx.send(pm).unwrap()
            // TODO: Send success
        } else {
            // TODO: Send fail
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {
        trace!("{}::Performing operation", self.id_str.clone());

        let x: f64 = random();

        if let Some(message) = request_option {
            if message.mtype == MessageType::CoordinatorPropose {
                // println!("coordinator proposed work to pariticpant and its completed");
                if x <= self.operation_success_prob {
                    self.log.append(
                        MessageType::ParticipantVoteCommit,
                        message.txid.clone(),
                        message.senderid.clone(),
                        message.opid.clone(),
                    );
                    return true;
                } else {
                    self.log.append(
                        MessageType::ParticipantVoteAbort,
                        message.txid.clone(),
                        message.senderid.clone(),
                        message.opid.clone(),
                    );
                    return false;
                }
            } else if (message.mtype == MessageType::CoordinatorCommit) {
                // println!("commited work");

                self.successful_ops += 1;
                return true;
            } else if message.mtype == MessageType::CoordinatorAbort {
                // println!("Coordinator asked participant to abort");

                self.failed_ops += 1;

                return true;
            } else {
                panic!(
                    "Recieved an invalid operation for a particapant {:?}",
                    message.mtype
                );
            }
        }
        // TODO: Successful operation

        return true;
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
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
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO

        trace!("{}::Exiting", self.id_str.clone());
    }

    pub fn recv_result(&mut self) -> Option<ProtocolMessage> {
        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        let res = match recieve_and_check_for_finish(&self.rx) {
            Some(m) => m,
            None => {
                self.running.store(false, Ordering::SeqCst);
                return None;
            }
        };

        // println!("Recieved result for particapant");
        return Some(res);

        // TODO
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());

        // println!("Starting to run protocol for participant");

        // TODO
        while self.running.load(Ordering::SeqCst) {
            let coord_res = match self.recv_result() {
                Some(m) => m,
                None => {
                    break;
                }
            };

            let opt = Some(coord_res);

            let was_completed = self.perform_operation(&opt);

            let msg_type = if was_completed {
                MessageType::ParticipantVoteCommit
            } else {
                MessageType::ParticipantVoteAbort
            };

            let pm = ProtocolMessage::generate(msg_type, self.id_str.clone(), "1".to_string(), 1);

            // send original status
            self.send(pm);

            // verify with the coordinator what the overall decision is
            // println!("verify with coordinator");
            let commit_res = match self.recv_result() {
                Some(m) => m,
                None => {
                    break;
                }
            };

            let opt_two = Some(commit_res);
            self.perform_operation(&opt_two);
        }

        self.wait_for_exit_signal();
        self.report_status();
    }
}

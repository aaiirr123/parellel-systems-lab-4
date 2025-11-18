//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::io::Empty;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use coordinator::ipc_channel::ipc::channel;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::TryRecvError;

use ipc_channel::ipc::IpcReceiverSet;
use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::{client, participant};

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision,
}

/// Coordinator
/// Struct maintaining state for coordinator
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    client_receiver_set: IpcReceiverSet,
    clients_sender_set: HashMap<u64, Sender<ProtocolMessage>>,
    particiapnt: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {
    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(log_path: String, r: &Arc<AtomicBool>) -> Coordinator {
        return Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            particiapnt: HashMap::new(), // TODO
            failed_ops: 0,
            successful_ops: 0,
            unknown_ops: 0,
            client_receiver_set: IpcReceiverSet::new().unwrap(),
            clients_sender_set: HashMap::new(),
        };
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        // println!("Joined as participant {}", name);
        self.particiapnt.insert(name.clone(), (tx, rx));
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        let id = self.client_receiver_set.add(rx).unwrap();
        self.clients_sender_set.insert(id, tx);

        // TODO
    }

    pub fn shutdown_client_join(&mut self, name: &String, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.client_receiver_set.add(rx).unwrap();
        // TODO
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats

        println!(
            "coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.successful_ops, self.failed_ops, self.unknown_ops
        );
    }

    pub fn send_client_result(
        &mut self,
        should_commit: bool,
        client_message: ProtocolMessage,
        id: u64,
    ) {
        // println!("waiting for client message");
        let client_tx: &Sender<ProtocolMessage> = self.clients_sender_set.get(&id).unwrap();

        let message_type;
        if should_commit {
            message_type = MessageType::ClientResultCommit;
        } else {
            message_type = MessageType::ClientResultAbort;
        }

        let client_msg = message::ProtocolMessage::generate(
            message_type,
            client_message.txid.clone(),
            client_message.senderid.clone(),
            client_message.opid.clone(),
        );

        client_tx.send(client_msg).unwrap();
    }

    pub fn send_particapant_work(&mut self, client_message: ProtocolMessage) -> bool {
        let participant_msg: ProtocolMessage = message::ProtocolMessage::generate(
            message::MessageType::CoordinatorPropose,
            client_message.txid.clone(),
            client_message.senderid.clone(),
            client_message.opid.clone(),
        );

        let mut should_commit = true;

        for (key, value) in &self.particiapnt {
            value.0.send(participant_msg.clone()).unwrap();

            let mut num_tries = 0;

            while self.running.load(Ordering::SeqCst) {
                match value.1.try_recv() {
                    Ok(message) => {
                        if message.mtype == MessageType::ParticipantVoteAbort {
                            should_commit = false;
                            break;
                        } else if message.mtype == MessageType::ParticipantVoteCommit {
                            // println!("Participant voted to commit");
                            break;
                        } else {
                            panic!("Invalid state: {:?}", message.mtype);
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        if (num_tries > 1) {
                            // println!("Did not recieve response from participant");
                            should_commit = false;
                            break;
                        }
                        thread::sleep(Duration::from_millis(1));
                        num_tries += 1;
                        continue;
                    }
                    Err(TryRecvError::IpcError(_)) => {
                        should_commit = false;
                        break;
                    }
                }
            }
        }

        return should_commit;
    }

    pub fn send_coordinator_cancel_alert(&mut self) {
        let exit_msg: ProtocolMessage = message::ProtocolMessage::generate(
            message::MessageType::CoordinatorExit,
            "coordinator".to_string(),
            "coordinator".to_string(),
            0,
        );

        for (key, value) in &self.clients_sender_set {
            value.send(exit_msg.clone()).unwrap();
        }
        for (key, value) in &self.particiapnt {
            value.0.send(exit_msg.clone()).unwrap();
        }
    }

    pub fn send_particapant_result(
        &mut self,
        should_commit: bool,
        client_message: ProtocolMessage,
    ) {
        let message_type;
        if should_commit {
            message_type = MessageType::CoordinatorCommit;
        } else {
            message_type = MessageType::CoordinatorAbort;
        }

        let participant_msg = message::ProtocolMessage::generate(
            message_type,
            client_message.txid.clone(),
            client_message.senderid.to_string(),
            client_message.opid,
        );

        self.log.append(
            message_type,
            client_message.txid,
            "coordinator".to_string(),
            client_message.opid,
        );

        for (key, value) in &self.particiapnt {
            value.0.send(participant_msg.clone()).unwrap();
        }
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        // TODO
        println!("running coordinator protocol");

        while self.running.load(Ordering::SeqCst) {
            for event in self.client_receiver_set.select().unwrap() {
                match event {
                    ipc_channel::ipc::IpcSelectionResult::MessageReceived(
                        id,
                        opaque_ipc_message,
                    ) => {
                        let client_message: ProtocolMessage = opaque_ipc_message.to().unwrap();
                        // println!("recieved request {:?}", client_message.clone());

                        if client_message.mtype == MessageType::ClientRequest {
                            // println!("Recieved a client request");

                            let should_commit: bool =
                                self.send_particapant_work(client_message.clone());
                            // println!("Coordinator wants to commit");
                            if should_commit {
                                self.successful_ops += 1;
                            } else {
                                self.failed_ops += 1;
                            }

                            self.send_particapant_result(should_commit, client_message.clone());
                            self.send_client_result(
                                should_commit,
                                client_message.clone(),
                                id.clone(),
                            );
                        }
                    }
                    ipc_channel::ipc::IpcSelectionResult::ChannelClosed(_) => {
                        continue;
                    }
                }
            }
        }

        self.send_coordinator_cancel_alert();

        println!("broke from the protocol");

        self.report_status();
    }
}

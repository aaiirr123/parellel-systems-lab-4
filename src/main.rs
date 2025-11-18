#[macro_use]
extern crate log;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
extern crate stderrlog;
use ipc_channel::ipc::channel;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcSender as Sender;
use std::env;
use std::fs;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub mod checker;
pub mod client;
pub mod coordinator;
pub mod message;
pub mod oplog;
pub mod participant;
pub mod tpcoptions;
use message::ProtocolMessage;

use crate::message::MessageType;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(
    child_opts: &mut tpcoptions::TPCOptions,
) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (server, server_name) =
        IpcOneShotServer::<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>::new().unwrap();

    child_opts.ipc_path = server_name.clone();
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    let (_, (tx_parent_to_child, rx_parent_from_child)) = server.accept().unwrap();

    // TODO

    (child, tx_parent_to_child, rx_parent_from_child)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(
    opts: &tpcoptions::TPCOptions,
) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let boot: Sender<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)> =
        Sender::connect(opts.ipc_path.clone()).unwrap();

    let (tx_child_to_parent, rx_parent_from_child) = channel().unwrap();
    let (tx_parent_to_child, rx_child_from_parent) = channel().unwrap();

    boot.send((tx_parent_to_child, rx_parent_from_child))
        .unwrap();

    // TODO

    (tx_child_to_parent, rx_child_from_parent)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(
    opts: &tpcoptions::TPCOptions,
    running: Arc<AtomicBool>,
    rx_shutdown: Receiver<ProtocolMessage>,
) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    // TODO
    // create coordinator
    let mut coord = coordinator::Coordinator::new(coord_log_path, &running);
    coord.shutdown_client_join(&"shutdown_client".to_string(), rx_shutdown);

    for i in 0..opts.num_clients {
        // now clients
        let mut child_opts = tpcoptions::TPCOptions::new();
        child_opts.mode = "client".to_string();
        child_opts.num_requests = opts.num_requests;
        child_opts.num = i;

        let spawn_test = spawn_child_and_connect(&mut child_opts);
        coord.client_join(&i.to_string(), spawn_test.1, spawn_test.2);
    }

    for i in 0..opts.num_participants {
        // now participants
        let mut participant_opts = tpcoptions::TPCOptions::new();
        participant_opts.mode = "participant".to_string();

        participant_opts.num = i;

        let par_test = spawn_child_and_connect(&mut participant_opts);

        coord.participant_join(&i.to_string(), par_test.1, par_test.2);
    }

    coord.protocol();
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: &tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let connectors: (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) =
        connect_to_coordinator(opts);
    let mut client = client::Client::new(opts.num.to_string(), running, connectors.0, connectors.1);
    client.protocol(opts.num_requests);

    // TODO
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: &tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let connectors: (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) =
        connect_to_coordinator(opts);
    let log_path = format!("{}//participant_{}.log", opts.log_path, opts.num);

    let mut participant = participant::Participant::new(
        participant_id_str,
        log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        connectors.0,
        connectors.1,
    );
    participant.protocol();

    // TODO
}

fn recieve_and_check_for_finish(rx: &Receiver<ProtocolMessage>) -> Option<ProtocolMessage> {
    match rx.recv() {
        Ok(val) => {
            if val.mtype == MessageType::CoordinatorExit {
                return None;
            }
            return Some(val);
        }
        Err(err) => {
            eprintln!("Disconnected client {:?}", err);
            return None;
        }
    }
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .verbosity(opts.verbosity)
        .init()
        .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!(
            "Failed to create log_path: \"{:?}\". Error \"{:?}\"",
            opts.log_path, e
        ),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    let (tx_shutdown, rx_shutdown): (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) =
        channel().unwrap();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
        let exit_msg: ProtocolMessage = message::ProtocolMessage::generate(
            message::MessageType::CoordinatorExit,
            "coordinator".to_string(),
            "coordinator".to_string(),
            0,
        );

        tx_shutdown.send(exit_msg).unwrap();
    })
    .expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running, rx_shutdown),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(
            opts.num_clients,
            opts.num_requests,
            opts.num_participants,
            &opts.log_path,
        ),
        _ => panic!("Unknown mode"),
    }
}

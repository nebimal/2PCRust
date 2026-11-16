#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;

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
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    
    // Okay so what I know is that child_ops is a struct
    // (input) -> (x, y, z) is the return of the function.

    /*********************
     *   1. Set up IPC   * 
    *********************/

    // Create a OneShotServer to connect the process to a server.
    let (server, server_name) =
        IpcOneShotServer::<(
            Sender<ProtocolMessage>,
            Receiver<ProtocolMessage>
        )>::new()
        .expect("Failed to create IPC one-shot server");
    

    // In the struct, it requires the path to the server.
    child_opts.ipc_path = server_name;

    /************************************************************ 
     *   2. Spawn a child process using the child CLI options   *   
    ************************************************************/

    // This spawns a process called child, which has the argument as a vector.
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");


    /******************************************************************************************** 
     *   3. Do any required communication to set up the parent / child communication channels   *   
    ********************************************************************************************/
    
    // Since this is the server process, we accept and block until a client sends a message.
     let (_rx, (tx, rx)) = server.accept()
        .expect("Failed to accept child connection");


    /**************************************************************************************** 
     *   4. Return the child process handle and the communication channels for the parent   *   
    ****************************************************************************************/

    (child, tx, rx)
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
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    /****************************************
     *   1. Connect to the parent via IPC   *
    ****************************************/

    // Create 2 channels 
    // Coordinator sends; child receives.
    let (parent_tx, child_rx) = channel::<ProtocolMessage>().unwrap();

    // Child sends; coordinator receives.
    let (child_tx, parent_rx) = channel::<ProtocolMessage>().unwrap();

    let bootstrap_tx = Sender::<(
    Sender<ProtocolMessage>,
    Receiver<ProtocolMessage>
    )>::connect(opts.ipc_path.clone())
    .expect("Failed to connect to coordinator");

    /********************************************************************************************
     *   2. Do any required communication to set up the parent / child communication channels   *
    ********************************************************************************************/
    
    // Sends the endpoints for the coordinator.
    // parent_tx: Allows the coordinator to send messages to the child.
    // parent_rx: Allows the coordinator to receive messages from the child.
    bootstrap_tx.send((parent_tx, parent_rx))
    .expect("Failed to send IPC endpoints");

    /**********************************************************
     *   3. Return the communication channels for the child   *
    **********************************************************/

    // Returns the endpoints that belong to the child.
    (child_tx, child_rx)
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
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    /************************************
     *   1. Creates a new coordinator   *
    ************************************/

    // Function in coordinator.rs handles this.
    let mut coordinator = coordinator::Coordinator::new(coord_log_path, &running);

    // Store child process handles, which allows the parent process manage 
    // and keep track of a child process it created.
    let mut children: Vec<Child> = Vec::new();

    /****************************************************************************************************
     *   2. Spawns and connects to new clients processes and then registers them with the coordinator   *
    ****************************************************************************************************/
    
    // I'm guessing I would do a for loop for num of clients the user specifies.
    for i in 0..opts.num_clients {
        // Gets the struct clone.
        let mut child_opts = opts.clone();

        // Since we're spawning client processes, we need to make sure we change their mode to 'client', 
        // so whenever the client process runs, it doesn't act as a coordinator.
        child_opts.mode = "client".to_string();

        // Assigns a unique Client's ID
        child_opts.num = i;

        // Spawns Client Processes
        let (child, tx, rx) = spawn_child_and_connect(&mut child_opts);

        // Give the client a unique name.
        let client_id = format!("client_{}", i);

        // Function in coordinator.rs which registers the client's communication channels.
        coordinator.client_join(&client_id, tx, rx);

        // Save the process handle for later.
        children.push(child);
    }

    /********************************************************************************************************
     *   3. Spawns and connects to new participant processes and then registers them with the coordinator   *
    ********************************************************************************************************/
    
    // Same as step 2 but with participants.
    for i in 0..opts.num_participants {
        // Gets the struct clone.
        let mut child_opts = opts.clone();

        // Since we're spawning participant processes, we need to make sure we change their mode to 'participant', 
        // so whenever the participant process runs, it doesn't act as a coordinator or client.
        child_opts.mode = "participant".to_string();

        // Assigns a unique Oarticipant's ID
        child_opts.num = i;

        // Spawns Participant Processes
        let (child, tx, rx) = spawn_child_and_connect(&mut child_opts);

        // Give the Participant a unique name.
        let participant_id = format!("participant_{}", i);

        // Function in coordinator.rs which registers the participant's communication channels.
        coordinator.participant_join(&participant_id, tx, rx);

        // Save the process handle for later.
        children.push(child);
    }

    /******************************************
     *   4. Starts the coordinator protocol   *
    ******************************************/

    // Function in coordinator.rs handles this.
    coordinator.protocol();

    /***************************************************
     *   5. Wait until the children finish execution   *
    ***************************************************/

    // Waits for all the child processes to finish.
    for mut child in children {
        child.wait().expect("Failed to wait for child process");
    }
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
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
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
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
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
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}

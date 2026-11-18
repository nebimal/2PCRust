//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
use message::MessageType;
use message::ProtocolMessage;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,

    // Communication with coordinator.
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,

    // Transaction statistics.
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
    pub fn new(id_str: String,
               running: Arc<AtomicBool>,
               tx: Sender<ProtocolMessage>,
               rx: Receiver<ProtocolMessage>) -> Client {
        Client {
            id_str: id_str,
            running: running,
            num_requests: 0,
            
            tx,
            rx,

            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            match self.rx.try_recv() {

                Ok(message) => {
                    if matches!(message.mtype, MessageType::CoordinatorExit) {
                        break;
                    }
                }

                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(10));
                }

                Err(error) => {
                    eprintln!("Exit signal receive error: {:?}", error);
                    break;
                }
            }
        }
        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) -> bool {

        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.num_requests);
        info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);

        match self.tx.send(pm) {
            Ok(()) => {
                trace!("{}::Sent operation #{}", self.id_str, self.num_requests);
                true
            }

            Err(error) => {
                eprintln!("Failed to send client request: {:?}", error);
                self.unknown_ops += 1;
                false
            }
        }

    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) -> bool {

        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        // The transaction ID of our most recent request.
        let expected_txid = format!(
            "{}_op_{}",
            self.id_str,
            self.num_requests
        );

        while self.running.load(Ordering::SeqCst) {
            match self.rx.try_recv() {

                Ok(result) => {
                    match result.mtype {

                        MessageType::ClientResultCommit
                            if result.txid == expected_txid =>
                        {
                            self.successful_ops += 1;
                            return true;
                        }

                        MessageType::ClientResultAbort
                            if result.txid == expected_txid =>
                        {
                            self.failed_ops += 1;
                            return true;
                        }

                        MessageType::CoordinatorExit => {
                            self.unknown_ops += 1;
                            return false;
                        }

                        _ => {
                            // Ignore unexpected messages.
                        }
                    }
                }

                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(10));
                }

                Err(error) => {
                    eprintln!("Failed to receive result: {:?}", error);
                    self.unknown_ops += 1;
                    return false;
                }
            }
        }

        // Interrupted before receiving a final result.
        self.unknown_ops += 1;
        false
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {

        let mut wait_for_exit = true;

        // Send the number of requests specified by the user.
        for _ in 0..n_requests {

            // Stop issuing requests if the simulation was interrupted.
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            // Send a transaction request.
            if !self.send_next_operation() {
                wait_for_exit = false;
                break;
            }

            // Wait for the transaction's result.
            if !self.recv_result() {
                wait_for_exit = false;
                break;
            }
        }

        // Wait for the coordinator's shutdown message unless
        // we already received it or lost the connection.
        if wait_for_exit {
            self.wait_for_exit_signal();
        }

        // Print transaction statistics.
        self.report_status();
    }
}

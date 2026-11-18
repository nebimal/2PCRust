//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::{HashMap, HashSet};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

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

    // Recovery: chance of crashing right after voting, and whether this
    // process is a restart that must rebuild its state from the log.
    crash_prob: f64,
    recovering: bool,

    // Communication with coordinator.
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,

    // Transaction statistics.
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
    /// A recovering participant reopens its existing log instead of starting
    /// a fresh one.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
        crash_prob: f64,
        recovering: bool) -> Participant {

        let log = if recovering {
            oplog::OpLog::from_file(log_path)
        } else {
            oplog::OpLog::new(log_path)
        };

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: log,
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,

            crash_prob: crash_prob,
            recovering: recovering,

            tx,
            rx,

            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x < self.send_success_prob {
            // Attempt to send the message.
            if let Err(error) = self.tx.send(pm) {
                eprintln!("Participant send failed: {:?}", error);
            }
        } else {
            // Simulate dropping the message.
            trace!("{}::Simulated message loss", self.id_str);
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {

        trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x < self.operation_success_prob {
            // Successful operation
            true
        } else {
            // Failed operation
            false
        }
    }

    ///
    /// maybe_crash()
    /// Simulates a crash with probability crash_prob. Called right after the
    /// vote is logged and sent, which is when a participant is "in doubt":
    /// it has promised to commit but doesn't know the outcome yet.
    ///
    fn maybe_crash(&self, txid: &Option<String>) {
        if self.crash_prob <= 0.0 {
            return;
        }
        let x: f64 = random();
        if x < self.crash_prob {
            eprintln!("{}::Simulated crash after voting on {:?}", self.id_str, txid);
            process::exit(1);
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    ///
    /// recover()
    /// Rebuilds state from the log after a restart.
    ///
    /// 1. Count the decisions already logged, so the final report covers
    ///    the whole run and not just this process.
    /// 2. Find transactions with a logged vote but no logged decision. These
    ///    are in doubt. (One that voted abort could safely abort on its own,
    ///    but asking the coordinator handles both cases the same way.)
    /// 3. Ask the coordinator for each outcome and log the answer.
    /// 4. Tell the coordinator recovery is finished.
    ///
    /// Recovery messages bypass the lossy send(), because -S simulates lost
    /// votes, not a broken recovery handshake.
    ///
    /// Returns false if the run is shutting down and the protocol loop
    /// should not start.
    ///
    pub fn recover(&mut self) -> bool {
        eprintln!("{}::Recovering from log", self.id_str);

        // Copy the entries out so the log lock isn't held while we work.
        let entries: Vec<ProtocolMessage> = {
            let log_arc = self.log.arc();
            let log = log_arc.lock().unwrap();
            let copied: Vec<ProtocolMessage> = log.values().cloned().collect();
            copied
        };

        let mut votes: HashMap<String, ProtocolMessage> = HashMap::new();
        let mut decided: HashSet<String> = HashSet::new();

        for entry in entries {
            match entry.mtype {
                MessageType::ParticipantVoteCommit | MessageType::ParticipantVoteAbort => {
                    votes.insert(entry.txid.clone(), entry);
                }
                MessageType::CoordinatorCommit => {
                    self.successful_ops += 1;
                    decided.insert(entry.txid.clone());
                }
                MessageType::CoordinatorAbort => {
                    self.failed_ops += 1;
                    decided.insert(entry.txid.clone());
                }
                _ => {}
            }
        }

        let mut keep_running = true;

        for (txid, vote) in votes {
            if decided.contains(&txid) {
                continue;
            }

            eprintln!("{}::In doubt about {} (voted {:?}), asking coordinator",
                      self.id_str, txid, vote.mtype);

            let query = ProtocolMessage::generate(
                MessageType::ParticipantRecoveryQuery,
                txid.clone(),
                self.id_str.clone(),
                vote.opid,
            );
            if let Err(error) = self.tx.send(query) {
                eprintln!("{}::Recovery query failed: {:?}", self.id_str, error);
                self.unknown_ops += 1;
                continue;
            }

            match self.wait_for_decision(&txid) {
                Some(decision) => {
                    // Log the outcome before acting on it, like any decision.
                    self.log.append(
                        decision,
                        txid.clone(),
                        self.id_str.clone(),
                        vote.opid,
                    );
                    if decision == MessageType::CoordinatorCommit {
                        self.successful_ops += 1;
                    } else {
                        self.failed_ops += 1;
                    }
                }
                None => {
                    self.unknown_ops += 1;
                    keep_running = false;
                    break;
                }
            }
        }

        let rejoin = ProtocolMessage::generate(
            MessageType::ParticipantRejoin,
            String::new(),
            self.id_str.clone(),
            0,
        );
        if let Err(error) = self.tx.send(rejoin) {
            eprintln!("{}::Rejoin failed: {:?}", self.id_str, error);
            return false;
        }

        keep_running
    }

    ///
    /// wait_for_decision()
    /// Waits for the coordinator's answer to a recovery query.
    /// Returns None if the run stops or the coordinator goes away first.
    ///
    fn wait_for_decision(&mut self, txid: &str) -> Option<MessageType> {
        while self.running.load(Ordering::SeqCst) {
            match self.rx.try_recv() {
                Ok(message) => {
                    if message.txid == txid
                        && (message.mtype == MessageType::CoordinatorCommit
                            || message.mtype == MessageType::CoordinatorAbort)
                    {
                        return Some(message.mtype);
                    }
                    if message.mtype == MessageType::CoordinatorExit {
                        return None;
                    }
                }

                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(10));
                }

                Err(error) => {
                    eprintln!("{}::Lost coordinator during recovery: {:?}", self.id_str, error);
                    return None;
                }
            }
        }
        None
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///

    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str);

        // A restarted participant settles its in-doubt transactions first.
        if self.recovering && !self.recover() {
            self.report_status();
            return;
        }

        // Track which transaction is awaiting a global decision.
        let mut current_txid: Option<String> = None;

        while self.running.load(Ordering::SeqCst) {

            match self.rx.try_recv() {
                Ok(message) => {

                    match message.mtype {

                        // STEP 1: Receive a proposal.
                        MessageType::CoordinatorPropose => {

                            // Coordinator handles one transaction at a time.
                            if current_txid.is_some() {
                                eprintln!("Already awaiting a decision");
                                continue;
                            }

                            self.state = ParticipantState::ReceivedP1;

                            // STEP 2: Simulate operation success/failure.
                            let success =
                                self.perform_operation(&Some(message.clone()));

                            // STEP 3: Decide which vote to send.
                            let vote_type = if success {
                                self.state = ParticipantState::VotedCommit;
                                MessageType::ParticipantVoteCommit
                            } else {
                                self.state = ParticipantState::VotedAbort;
                                MessageType::ParticipantVoteAbort
                            };

                            // STEP 4: Record the vote locally.
                            self.log.append(
                                vote_type.clone(),
                                message.txid.clone(),
                                self.id_str.clone(),
                                message.opid,
                            );

                            // STEP 5: Construct and send the vote.
                            let vote = ProtocolMessage::generate(
                                vote_type,
                                message.txid.clone(),
                                self.id_str.clone(),
                                message.opid,
                            );

                            current_txid = Some(message.txid);
                            self.send(vote);

                            // Crash injection: the vote is logged and sent,
                            // but the decision hasn't arrived yet.
                            self.maybe_crash(&current_txid);

                            self.state = ParticipantState::AwaitingGlobalDecision;
                        }

                        // STEP 6: Receive global COMMIT.
                        MessageType::CoordinatorCommit => {
                            if current_txid.as_deref()
                                != Some(message.txid.as_str())
                            {
                                continue;
                            }

                            self.log.append(
                                MessageType::CoordinatorCommit,
                                message.txid,
                                self.id_str.clone(),
                                message.opid,
                            );

                            self.successful_ops += 1;
                            current_txid = None;
                            self.state = ParticipantState::Quiescent;
                        }

                        // STEP 7: Receive global ABORT.
                        MessageType::CoordinatorAbort => {
                            if current_txid.as_deref()
                                != Some(message.txid.as_str())
                            {
                                continue;
                            }

                            self.log.append(
                                MessageType::CoordinatorAbort,
                                message.txid,
                                self.id_str.clone(),
                                message.opid,
                            );

                            self.failed_ops += 1;
                            current_txid = None;
                            self.state = ParticipantState::Quiescent;
                        }

                        // STEP 8: Exit when coordinator requests shutdown.
                        MessageType::CoordinatorExit => {
                            break;
                        }

                        _ => {
                            // Ignore unexpected message types.
                        }
                    }
                }

                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(10));
                }

                Err(error) => {
                    eprintln!("Participant receive error: {:?}", error);
                    break;
                }
            }
        }

        // A proposed transaction never received its global decision.
        if current_txid.is_some() {
            self.unknown_ops += 1;
        }

        self.report_status();
    }

}

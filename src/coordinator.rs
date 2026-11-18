//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::process::Child;
use std::fmt;
use std::mem;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::rand::random;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

/// How long the coordinator waits for all votes before aborting.
const VOTE_TIMEOUT_MS: u64 = 1000;

/// How long the coordinator waits for a restarted participant to finish recovery.
const REJOIN_TIMEOUT_MS: u64 = 2000;

/// Restarts participant number N and returns its process handle and channels.
/// main.rs supplies this, since it knows how to spawn child processes.
pub type RespawnFn = Box<dyn FnMut(u32) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>;

/// Holds the respawn function. Closures can't derive Debug, so this wrapper
/// implements it by hand to keep #[derive(Debug)] working on Coordinator.
struct Respawner(Option<RespawnFn>);

impl fmt::Debug for Respawner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Respawner({})", if self.0.is_some() { "set" } else { "unset" })
    }
}

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,


    /* For the Coordinator, it would need to know what clients/participants it has a communication channel with */
    /* Meaning we need to also keep in track of the channels connected with clients/participants from coordinator */
    
    // Client communication channels.
    client_senders: HashMap<String, Sender<ProtocolMessage>>,
    client_receivers: HashMap<String, Receiver<ProtocolMessage>>,

    // Participant communication channels.
    participant_senders: HashMap<String, Sender<ProtocolMessage>>,
    participant_receivers: HashMap<String, Receiver<ProtocolMessage>>,

    // Transaction statistics.
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,

    // Recovery: participants whose channel has closed, how to restart them,
    // and the process handles of restarted participants (main.rs waits on these).
    dead: HashSet<String>,
    respawner: Respawner,
    respawned: Vec<Child>,

    // Coordinator failure: where the log lives (recovery reloads it from disk)
    // and the chance of a simulated crash at each crash point.
    log_path: String,
    crash_prob: f64,
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
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path.clone()),
            log_path: log_path,
            crash_prob: 0.0,
            running: r.clone(),
            
            client_senders: HashMap::new(),
            client_receivers: HashMap::new(),

            participant_senders: HashMap::new(),
            participant_receivers: HashMap::new(),

            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,

            dead: HashSet::new(),
            respawner: Respawner(None),
            respawned: Vec::new(),
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, 
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // Register's the channels endpoints.
        self.participant_senders.insert(name.clone(), tx);
        self.participant_receivers.insert(name.clone(), rx);
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, 
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // Register's the channels endpoints.
        self.client_senders.insert(name.clone(), tx);
        self.client_receivers.insert(name.clone(), rx);
    }

    ///
    /// set_crash_probability()
    /// Chance of a simulated coordinator crash at each crash point.
    ///
    pub fn set_crash_probability(&mut self, p: f64) {
        self.crash_prob = p;
    }

    ///
    /// maybe_crash()
    /// Returns true if the coordinator should simulate a crash at this point.
    ///
    fn maybe_crash(&self, point: &str) -> bool {
        if self.crash_prob <= 0.0 {
            return false;
        }
        let x: f64 = random();
        if x < self.crash_prob {
            eprintln!("coordinator::Simulated crash {}", point);
            return true;
        }
        false
    }

    ///
    /// recover_coordinator()
    /// Simulates a coordinator restart. Everything in memory about the current
    /// transaction is gone; the coordinator rebuilds its state from the log
    /// on disk. (The IPC channels are kept, standing in for a restarted
    /// coordinator reconnecting to the same clients and participants.)
    ///
    /// The coordinator runs one transaction at a time, so only the most
    /// recently proposed transaction can be unfinished:
    ///   - decision logged:     resend it (it may not have reached everyone)
    ///   - no decision logged:  presumed abort -- log abort, then send it
    /// Participants ignore a decision for a transaction they aren't waiting
    /// on, so resending is safe.
    ///
    /// Returns the number of transactions finished so far, recomputed from
    /// the log.
    ///
    fn recover_coordinator(&mut self) -> u32 {
        eprintln!("coordinator::Recovering from log");
        self.state = CoordinatorState::Quiescent;

        // Reload the log from disk, as a restarted process would.
        self.log = oplog::OpLog::from_file(self.log_path.clone());

        let entries: Vec<ProtocolMessage> = {
            let log_arc = self.log.arc();
            let log = log_arc.lock().unwrap();
            let copied: Vec<ProtocolMessage> = log.values().cloned().collect();
            copied
        };

        // Rebuild statistics and find the last proposal.
        self.successful_ops = 0;
        self.failed_ops = 0;
        let mut decisions: HashMap<String, MessageType> = HashMap::new();
        let mut last_proposal: Option<ProtocolMessage> = None;

        for entry in entries {
            match entry.mtype {
                MessageType::CoordinatorCommit => {
                    self.successful_ops += 1;
                    decisions.insert(entry.txid.clone(), entry.mtype);
                }
                MessageType::CoordinatorAbort => {
                    self.failed_ops += 1;
                    decisions.insert(entry.txid.clone(), entry.mtype);
                }
                MessageType::CoordinatorPropose => {
                    let newer = match last_proposal {
                        Some(ref p) => entry.uid > p.uid,
                        None => true,
                    };
                    if newer {
                        last_proposal = Some(entry);
                    }
                }
                _ => {}
            }
        }

        if let Some(proposal) = last_proposal {
            let decision = match decisions.get(&proposal.txid) {
                Some(d) => *d,
                None => {
                    // Never decided, so nobody can have committed: presumed abort.
                    // Log it before sending it, like any decision.
                    self.log.append(
                        MessageType::CoordinatorAbort,
                        proposal.txid.clone(),
                        "coordinator".to_string(),
                        proposal.opid,
                    );
                    self.failed_ops += 1;
                    MessageType::CoordinatorAbort
                }
            };

            eprintln!("coordinator::Finishing {} after restart with {:?}",
                      proposal.txid, decision);

            // Resend the decision to every participant.
            let decision_message = ProtocolMessage::generate(
                decision,
                proposal.txid.clone(),
                "coordinator".to_string(),
                proposal.opid,
            );
            let mut gone: Vec<String> = Vec::new();
            for (participant_id, participant_send) in &self.participant_senders {
                if let Err(error) = participant_send.send(decision_message.clone()) {
                    eprintln!("Failed to send decision to {}: {:?}", participant_id, error);
                    gone.push(participant_id.clone());
                }
            }
            for id in gone {
                self.dead.insert(id);
            }

            // The client never got its result before the crash. The proposal
            // record's sender is the client that made the request.
            let result_type = if decision == MessageType::CoordinatorCommit {
                MessageType::ClientResultCommit
            } else {
                MessageType::ClientResultAbort
            };
            let result_message = ProtocolMessage::generate(
                result_type,
                proposal.txid.clone(),
                "coordinator".to_string(),
                proposal.opid,
            );
            match self.client_senders.get(&proposal.senderid) {
                Some(tx) => {
                    if let Err(error) = tx.send(result_message) {
                        eprintln!("Could not send result to {}: {:?}", proposal.senderid, error);
                    }
                }
                None => {
                    eprintln!("Client {} is no longer connected", proposal.senderid);
                }
            }
        }

        (self.successful_ops + self.failed_ops) as u32
    }

    ///
    /// set_respawner()
    /// Gives the coordinator a way to restart crashed participants.
    ///
    pub fn set_respawner(&mut self, respawn: RespawnFn) {
        self.respawner = Respawner(Some(respawn));
    }

    ///
    /// take_respawned()
    /// Hands the process handles of restarted participants to main.rs so it
    /// can wait for them to exit.
    ///
    pub fn take_respawned(&mut self) -> Vec<Child> {
        mem::replace(&mut self.respawned, Vec::new())
    }

    ///
    /// find_dead_participants()
    /// Called between transactions. Drains stale messages (votes left over from
    /// an earlier transaction) and marks any participant whose channel has
    /// closed as dead.
    ///
    fn find_dead_participants(&mut self) {
        let mut gone: Vec<String> = Vec::new();

        for (participant_id, participant_recv) in &self.participant_receivers {
            loop {
                match participant_recv.try_recv() {
                    // Stale message from an earlier transaction: discard it.
                    Ok(_) => continue,
                    Err(TryRecvError::Empty) => break,
                    Err(_) => {
                        gone.push(participant_id.clone());
                        break;
                    }
                }
            }
        }

        for id in gone {
            self.dead.insert(id);
        }
    }

    ///
    /// respawn_dead_participants()
    /// Restarts every participant marked dead, one at a time.
    ///
    fn respawn_dead_participants(&mut self) {
        if self.respawner.0.is_none() || !self.running.load(Ordering::SeqCst) {
            return;
        }

        let mut names: Vec<String> = self.dead.iter().cloned().collect();
        names.sort();
        for name in names {
            self.rejoin_participant(&name);
        }
    }

    ///
    /// rejoin_participant()
    /// Restarts one participant in recovery mode, answers its recovery queries
    /// from the coordinator log, and registers its new channels.
    ///
    /// Recovery handshake:
    ///   participant -> ParticipantRecoveryQuery(txid)   (zero or more)
    ///   coordinator -> CoordinatorCommit / CoordinatorAbort for that txid
    ///   participant -> ParticipantRejoin                 (exactly once, at the end)
    ///
    fn rejoin_participant(&mut self, name: &String) {
        let num = match name.trim_start_matches("participant_").parse::<u32>() {
            Ok(n) => n,
            Err(_) => return,
        };

        eprintln!("coordinator::Restarting {}", name);
        let (child, tx, rx) = match self.respawner.0.as_mut() {
            Some(respawn) => respawn(num),
            None => return,
        };

        let deadline = Instant::now() + Duration::from_millis(REJOIN_TIMEOUT_MS);
        let mut finished = false;

        while !finished && Instant::now() < deadline {
            match rx.try_recv() {
                Ok(message) => match message.mtype {
                    MessageType::ParticipantRecoveryQuery => {
                        let decision = self.logged_decision(&message.txid);
                        eprintln!("coordinator::{} asked about {}, answering {:?}",
                                  name, message.txid, decision);

                        let reply = ProtocolMessage::generate(
                            decision,
                            message.txid.clone(),
                            "coordinator".to_string(),
                            message.opid,
                        );
                        if let Err(error) = tx.send(reply) {
                            eprintln!("Could not answer {}: {:?}", name, error);
                            finished = true;
                        }
                    }

                    MessageType::ParticipantRejoin => {
                        finished = true;
                    }

                    _ => {
                        // Nothing else is expected during recovery.
                    }
                },

                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(10));
                }

                Err(error) => {
                    eprintln!("{} failed during recovery: {:?}", name, error);
                    finished = true;
                }
            }
        }

        if !finished {
            eprintln!("{} did not finish recovery in time", name);
        }

        // Replace the dead channels with the new ones. If the new process has
        // also failed, the next transaction will notice and try again.
        self.participant_senders.insert(name.clone(), tx);
        self.participant_receivers.insert(name.clone(), rx);
        self.respawned.push(child);
        self.dead.remove(name);
    }

    ///
    /// logged_decision()
    /// Looks up the global decision for txid in the coordinator log. If there
    /// is none, the transaction never committed, so the answer is abort
    /// (presumed abort).
    ///
    fn logged_decision(&self, txid: &str) -> MessageType {
        let log_arc = self.log.arc();
        let log = log_arc.lock().unwrap();

        for entry in log.values() {
            if entry.txid == txid
                && (entry.mtype == MessageType::CoordinatorCommit
                    || entry.mtype == MessageType::CoordinatorAbort)
            {
                return entry.mtype;
            }
        }

        MessageType::CoordinatorAbort
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///

  
    /*
    The coordinator is responsible for receiving transaction requests
    from clients and coordinating the participants.

    For example, if a customer buys something on Amazon:
    - The client requests a purchase.
    - The coordinator asks participants (inventory, payment, etc.)
      whether they can successfully complete their operations.
    - Each participant votes commit or abort.
    - The coordinator makes the final decision.

    Steps for Coordinator::protocol():

    1. Keep checking for requests from clients until one is received.
    2. Once a ClientRequest is received, send a CoordinatorPropose
       message to ALL participants.
    3. Wait for participants to respond with ParticipantVoteCommit
       or ParticipantVoteAbort. Remember to handle timeouts.
    4. Check the votes:
       - If ALL participants vote commit, decide to COMMIT.
       - If ANY participant votes abort, decide to ABORT.
       - If any required vote is missing after the timeout,
         decide to ABORT.
    5. Record the global decision in the coordinator's log.
    6. Send CoordinatorCommit or CoordinatorAbort to ALL participants.
    7. Send ClientResultCommit or ClientResultAbort to the
       specific client that originally requested the transaction.
    8. Return to waiting for the next client request.
    9. When all requests have been processed or the simulation
       is shutting down, send CoordinatorExit messages and
       report the final statistics.
    */

    pub fn protocol(&mut self, total_requests: u32) {

        // Alright so for this, I would need to use try_recv because if I use recv,
        // it will block while using a try_recv will always return a value without blocking.
        // With try, it has 2 failure cases, either disconnection and empty buffer(which recv does not have).
        // pub fn try_recv(&self) -> Result<T, TryRecvError>


        // Keep running until all transactions are completed
        // or the simulation is stopped.
        let mut completed_requests: u32 = 0;
        while self.running.load(Ordering::SeqCst) && completed_requests < total_requests {

            /************************************************************************
             *   1. Keep checking for requests from clients until one is received   *
            ************************************************************************/

            // Stores the request and its originating client.
            let mut pending_request: Option<(String, ProtocolMessage)> = None;

            // Clients whose channel has closed during this pass.
            let mut gone: Vec<String> = Vec::new();

            for (client_id, client_recv) in &self.client_receivers {

                match client_recv.try_recv() {
                    Ok(request) => {
                        if matches!(request.mtype, MessageType::ClientRequest) {
                            pending_request = Some((client_id.clone(), request));

                            // Exit for loop since we received a request.
                            break;
                        }
                    }

                    Err(TryRecvError::Empty) => {
                        // Nothing received from this client.
                    }

                    Err(error) => {
                        // The client's channel is closed; stop polling it.
                        eprintln!("Client {} disconnected: {:?}", client_id, error);
                        gone.push(client_id.clone());
                    }
                }
            }

            // Forget disconnected clients so we never poll them again.
            for id in gone {
                self.client_receivers.remove(&id);
                self.client_senders.remove(&id);
            }

            // With no clients left, no more requests can ever arrive.
            if self.client_receivers.is_empty() {
                break;
            }

            // If no request was received, skip to the next while iteration.
            if pending_request.is_none() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }

            // let mut status = RequestStatus::Unknown;
            // Since a request exists, retrieve the request and the client who requested it.
            let (client_id, request) = pending_request.unwrap();

            // Bring back any participant that crashed since the last transaction,
            // so it can recover before voting on this one.
            self.find_dead_participants();
            self.respawn_dead_participants();

            // Set the state of the coordinator to ReceivedRequiest since we received a request.
            self.state = CoordinatorState::ReceivedRequest;

            /**************************************************************************************************
             *   2. Once a ClientRequest is received, send a CoordinatorPropose message to ALL participants   *
            **************************************************************************************************/

            // Create a proposal message from the client's request.
            let proposal = ProtocolMessage::generate(
                MessageType::CoordinatorPropose,
                request.txid.clone(),
                "coordinator".to_string(),
                request.opid,
            );

            // Log the proposal before sending it, so a restarted coordinator
            // knows this transaction was in progress. The sender field records
            // which client asked for it. (The checker only counts commit and
            // abort records, so this extra record doesn't affect it.)
            self.log.append(
                MessageType::CoordinatorPropose,
                request.txid.clone(),
                client_id.clone(),
                request.opid,
            );

            // Participants found dead during this transaction.
            let mut newly_dead: Vec<String> = Vec::new();

            // Send proposal to all participants.
            for (participant_id, participant_send) in &self.participant_senders {
                if let Err(error) = participant_send.send(proposal.clone()) {
                    // No vote can come from this participant, so the vote
                    // count below will abort the transaction.
                    eprintln!("Could not reach {}: {:?}", participant_id, error);
                    newly_dead.push(participant_id.clone());
                }
            }
            self.state = CoordinatorState::ProposalSent;

            /********************************************************************************************** 
             *   3. Wait for participants to respond with ParticipantVoteCommit or ParticipantVoteAbort   * 
             *   Remember to handle timeouts.                                                             *
            **********************************************************************************************/
            // Give the entire voting phase one second.
            let deadline = Instant::now() + Duration::from_millis(VOTE_TIMEOUT_MS);

            // Track which participants voted commit for this transaction.
            let mut commit_voters: HashSet<String> = HashSet::new();    
            
            // Assume commit is possible until an abort or failure occurs.
            let mut decision = MessageType::CoordinatorCommit;

            while self.running.load(Ordering::SeqCst)
                && Instant::now() < deadline
                && commit_voters.len() < self.participant_receivers.len()
            {
                for (participant_id, participant_recv) in &self.participant_receivers {

                    // Already received this participant's vote.
                    if commit_voters.contains(participant_id) {
                        continue;
                    }

                    match participant_recv.try_recv() {
                        Ok(vote_result) => {

                            // Make sure this vote belongs to our transaction.
                            if vote_result.txid != request.txid {
                                continue;
                            }

                            match vote_result.mtype {
                                MessageType::ParticipantVoteCommit => {
                                    commit_voters.insert(participant_id.clone());
                                }

                                MessageType::ParticipantVoteAbort => {
                                    decision = MessageType::CoordinatorAbort;
                                    break;
                                }

                                _ => {
                                    // Unexpected message.
                                }
                            }
                        }

                        Err(TryRecvError::Empty) => {
                            // No vote available yet.
                        }

                        Err(error) => {
                            eprintln!("{} disconnected: {:?}", participant_id, error);
                            newly_dead.push(participant_id.clone());
                            decision = MessageType::CoordinatorAbort;
                            break;
                        }
                    }
                }

                // Stop collecting votes after an abort.
                if matches!(decision, MessageType::CoordinatorAbort) {
                    break;
                }

                thread::sleep(Duration::from_millis(10));
            }
                
            /*************************************************************      
             *   4. Check the votes:                                     *
             *     - If ALL participants vote commit, decide to COMMIT   *
             *     - If ANY participant votes abort, decide to ABORT     *
             *     - If any required vote is missing after the timeout   *
             *       decide to ABORT.                                    *
            *************************************************************/

            // If any required commit vote is missing, abort.
            if !self.running.load(Ordering::SeqCst) || commit_voters.len() != self.participant_receivers.len() {
                decision = MessageType::CoordinatorAbort;
            }

            // Crash point 1: votes are in, but no decision is logged yet.
            if self.maybe_crash("before logging the decision") {
                completed_requests = self.recover_coordinator();
                continue;
            }

            // Update the coordinator's state.
            match decision {
                MessageType::CoordinatorCommit => {
                    self.state = CoordinatorState::ReceivedVotesCommit;
                    // status = RequestStatus::Committed;
                    self.successful_ops += 1;
                }

                MessageType::CoordinatorAbort => {
                    self.state = CoordinatorState::ReceivedVotesAbort;
                    // status = RequestStatus::Aborted;
                    self.failed_ops += 1;
                }

                _ => {
                    // status = RequestStatus::Unknown;
                    self.unknown_ops += 1;
                    unreachable!();
                }
            }
            /**************************************************************
             *   5. Record the global decision in the coordinator's log   *
            **************************************************************/
            self.log.append(
                decision.clone(),
                request.txid.clone(),
                "coordinator".to_string(),
                request.opid,
            );
            /*************************************************************************
             *   6. Send CoordinatorCommit or CoordinatorAbort to ALL participants   *
            *************************************************************************/

            // Crash point 2: the decision is logged but nobody has heard it.
            if self.maybe_crash("after logging the decision, before sending it") {
                completed_requests = self.recover_coordinator();
                continue;
            }

            // Create a message containing the final decision.
            let decision_message = ProtocolMessage::generate(
                decision.clone(),
                request.txid.clone(),
                "coordinator".to_string(),
                request.opid,
            );

            // Send the global decision to every participant.
            let mut sent_count = 0;
            let mut crashed = false;
            for (participant_id, participant_send) in &self.participant_senders {
                // Crash point 3: some participants have the decision, others don't.
                if sent_count == 1 && self.maybe_crash("after sending the decision to some participants") {
                    crashed = true;
                    break;
                }
                sent_count += 1;

                if let Err(error) = participant_send.send(decision_message.clone()) {
                    eprintln!(
                        "Failed to send decision to {}: {:?}",
                        participant_id,
                        error
                    );
                    newly_dead.push(participant_id.clone());
                }
            }
            

            /************************************************************************************************************************
             *   7. Send ClientResultCommit or ClientResultAbort to the specific client that originally requested the transaction   *
            ************************************************************************************************************************/
            
            // Remember who failed so they are restarted before the next transaction.
            for id in newly_dead {
                self.dead.insert(id);
            }

            if crashed {
                completed_requests = self.recover_coordinator();
                continue;
            }

            // Find the original client's sender.
            let client_send = self.client_senders.get(&client_id);

            // Determine which result to send.
            let result_type = match decision {
                MessageType::CoordinatorCommit => {
                    MessageType::ClientResultCommit
                }

                MessageType::CoordinatorAbort => {
                    MessageType::ClientResultAbort
                }

                _ => unreachable!(),
            };

            // Construct the result message.
            let result_message = ProtocolMessage::generate(
                result_type,
                request.txid.clone(),
                "coordinator".to_string(),
                request.opid,
            );

            // Send result to the requesting client.
            match client_send {
                Some(tx) => {
                    if let Err(error) = tx.send(result_message) {
                        eprintln!("Could not send result to {}: {:?}", client_id, error);
                    }
                }
                None => {
                    eprintln!("Client {} is no longer connected", client_id);
                }
            }

            // Update coordinator state.
            self.state = CoordinatorState::SentGlobalDecision;

            /********************************************************
             *   8. Return to waiting for the next client request   *
            ********************************************************/
            // One transaction finished.
            completed_requests += 1;

            // Coordinator is ready to receive another request.
            self.state = CoordinatorState::Quiescent;

        }
        /************************************************************************************
         *   9. When all requests have been processed or the simulation is shutting down,   * 
         *   send CoordinatorExit messages and report the final statistics.                 *
        ************************************************************************************/
        
        // A participant that crashed during the last transaction still needs to
        // recover its in-doubt transaction before everyone shuts down.
        if self.running.load(Ordering::SeqCst) {
            self.find_dead_participants();
            self.respawn_dead_participants();
        }

        // Create a shutdown message.
        let exit_message = ProtocolMessage::generate(
            MessageType::CoordinatorExit,
            "shutdown".to_string(),
            "coordinator".to_string(),
            0,
        );

        // Tell all clients to exit.
        for (client_id, client_send) in &self.client_senders {
            if let Err(error) = client_send.send(exit_message.clone()) {
                eprintln!("Failed to notify {}: {:?}", client_id, error);
            }
        }

        // Tell all participants to exit.
        for (participant_id, participant_send) in &self.participant_senders {
            if let Err(error) = participant_send.send(exit_message.clone()) {
                eprintln!("Failed to notify {}: {:?}", participant_id, error);
            }
        }

        self.report_status();
    }
}

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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;
use tpcoptions;

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
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            
            client_senders: HashMap::new(),
            client_receivers: HashMap::new(),

            participant_senders: HashMap::new(),
            participant_receivers: HashMap::new(),

            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
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
                        eprintln!("Error: {:?}", error);
                    }
                }
            }

            // If no request was received, skip to the next while iteration.
            if pending_request.is_none() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }

            // let mut status = RequestStatus::Unknown;
            // Since a request exists, retrieve the request and the client who requested it.
            let (client_id, request) = pending_request.unwrap();

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

            // Send proposal to all participants.
            for (_participant_id, participant_send) in &self.participant_senders {
                participant_send.send(proposal.clone()).unwrap();
            }
            self.state = CoordinatorState::ProposalSent;

            /********************************************************************************************** 
             *   3. Wait for participants to respond with ParticipantVoteCommit or ParticipantVoteAbort   * 
             *   Remember to handle timeouts.                                                             *
            **********************************************************************************************/
            // Give the entire voting phase one second.
            let deadline = Instant::now() + Duration::from_millis(1000);

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
                            eprintln!("Receive error: {:?}", error);
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

            // Create a message containing the final decision.
            let decision_message = ProtocolMessage::generate(
                decision.clone(),
                request.txid.clone(),
                "coordinator".to_string(),
                request.opid,
            );

            // Send the global decision to every participant.
            for (participant_id, participant_send) in &self.participant_senders {
                if let Err(error) = participant_send.send(decision_message.clone()) {
                    eprintln!(
                        "Failed to send decision to {}: {:?}",
                        participant_id,
                        error
                    );
                }
            }
            

            /************************************************************************************************************************
             *   7. Send ClientResultCommit or ClientResultAbort to the specific client that originally requested the transaction   *
            ************************************************************************************************************************/
            
            // Find the original client's sender.
            let client_send = self.client_senders.get(&client_id).unwrap();

            // Determine which result to send.
            let result_type = match self.state {
                CoordinatorState::ReceivedVotesCommit => {
                    MessageType::ClientResultCommit
                }

                CoordinatorState::ReceivedVotesAbort => {
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
            client_send.send(result_message).unwrap();

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

//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

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
        // TODO: Collect actual stats
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
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

    pub fn protocol(&mut self) {

        // Alright so for this, I would need to use try_recv because if I use recv,
        // it will block while using a try_recv will always return a value without blocking.
        // With try, it has 2 failure cases, either disconnection and empty buffer(which recv does not have).
        // pub fn try_recv(&self) -> Result<T, TryRecvError>


        // Keep running until all transactions are completed
        // or the simulation is stopped.
        let opts = tpcoptions::TPCOptions::new();
        let total_requests = opts.num_clients * opts.num_requests;
        while self.running.load(Ordering::SeqCst) && completed_requests < total_requests {

            /************************************************************************
             *   1. Keep checking for requests from clients until one is received   *
            ************************************************************************/

            // Stores the request and its originating client.
            let mut pending_request: Option<(String, ProtocolMessage)> = None;

            for (client_id, client_recv) in &self.client_receivers {

                match client_recv.try_recv() {
                    Ok(request) => {
                        println!(
                            "Received request {} from {}",
                            request.txid,
                            client_id
                        );

                        pending_request = Some((client_id.clone(), request));

                        // Exit for loop since we received a request.
                        break;
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

            // Since a request exists, retrieve the request and the client who requested it.
            let (client_id, request) = pending_request.unwrap();

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

            /********************************************************************************************** 
             *   3. Wait for participants to respond with ParticipantVoteCommit or ParticipantVoteAbort   * 
             *   Remember to handle timeouts.                                                             *
            **********************************************************************************************/

            // STEP 3 IS NOT FINISH, DO THIS!!!!!!!!!!!!!!!! 
            
            for (_participant_id, participant_recv) in &self.participant_receivers {
                
                match participant_recv.try_recv() {
                    Ok(voteResult) => {
                        // Voting results
                        println!(
                            "Received {} from {}",
                            voteResult.txid,
                            participant_id
                        );

                        pending_request = Some((client_id.clone(), request));

                        // Exit for loop since we received a request.
                        break;
                    }

                    Err(TryRecvError::Empty) => {
                        // Nothing received from this client.
                    }

                    Err(error) => {
                        eprintln!("Error: {:?}", error);
                    }
                }
            }




        }


        self.report_status();
    }
}

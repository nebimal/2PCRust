extern crate serde;
extern crate serde_json;
extern crate bincode;

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;

use message;

#[derive(Debug)]
pub struct OpLog {
    seqno: u32,
    log_arc: Arc<Mutex<HashMap<u32, message::ProtocolMessage>>>,
    path: String,
    lf: File,
}

impl OpLog {

    ///
    /// new(fpath: String)
    /// Creates a new Operations Log at the designated file path
    ///
    pub fn new(fpath: String) -> OpLog {
        let l = HashMap::new();
        let lck = Mutex::new(l);
        let arc = Arc::new(lck);
        OpLog {
            seqno: 0,
            log_arc: arc,
            path: fpath.to_string(),
            lf: File::create(fpath).unwrap(),
        }
    }

    ///
    /// from_file(fpath: String)
    ///
    /// Reads in and returns an existing Operations Log from the designated file.
    /// A partially written final line (for example from a crash mid-write) is
    /// ignored instead of crashing the reader. The file is reopened in append
    /// mode so the returned log can keep growing.
    ///
    pub fn from_file(fpath: String) -> OpLog {
        let mut seqno = 0;
        let mut l = HashMap::new();
        let scopy = fpath.clone();
        let tlf = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&fpath)
            .unwrap();
        {
            let mut reader = BufReader::new(&tlf);
            let mut line = String::new();
            let mut len = reader.read_line(&mut line).unwrap();
            while len > 0 {
                if !line.trim().is_empty() {
                    match serde_json::from_str::<message::ProtocolMessage>(line.trim_end()) {
                        Ok(pm) => {
                            if pm.uid > seqno {
                                seqno = pm.uid;
                            }
                            l.insert(pm.uid, pm);
                        }
                        Err(_) => {
                            // Torn write at the end of the log: stop here.
                            break;
                        }
                    }
                }
                line.clear();
                len = reader.read_line(&mut line).unwrap();
            }
        }
        let lck = Mutex::new(l);
        let arc = Arc::new(lck);
        OpLog {
            seqno: seqno,
            log_arc: arc,
            path: scopy,
            lf: tlf,
        }
    }

    ///
    /// append(t, tid, sender, op)
    ///
    /// Appends an entry to the Operations Log. The entry is written with a
    /// single write call and synced to disk before it is recorded in memory,
    /// so anything visible in the in-memory log is also durable.
    ///
    pub fn append(&mut self, t: message::MessageType, tid: String, sender: String, op: u32) {
        let lck = Arc::clone(&self.log_arc);
        let mut log = lck.lock().unwrap();
        
        let pm = message::ProtocolMessage::generate(t, tid, sender, op);

        // One buffer, one write: a crash can't leave half a JSON object behind
        // because of several small writes.
        let mut line = serde_json::to_string(&pm).unwrap();
        line.push('\n');
        self.lf.write_all(line.as_bytes()).unwrap();
        self.lf.sync_all().unwrap();

        // Key by uid so live logs and logs reloaded by from_file agree.
        if pm.uid > self.seqno {
            self.seqno = pm.uid;
        }
        log.insert(pm.uid, pm);
    }

    ///
    /// read(offset)
    ///
    /// Returns the log entry with the given uid
    ///
    pub fn read(&mut self, offset: &u32) -> message::ProtocolMessage {
        let lck = Arc::clone(&self.log_arc);
        let log = lck.lock().unwrap();
        let pm = log[&offset].clone();
        pm
    }

    ///
    /// arc
    ///
    /// Returns an ARC of the mutex locked log
    ///
    pub fn arc(&self) -> Arc<Mutex<HashMap<u32, message::ProtocolMessage>>> {
        Arc::clone(&self.log_arc)
    }
}

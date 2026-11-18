# Two-Phase Commit in Rust

A two-phase commit (2PC) protocol with one coordinator, any number of clients
issuing transaction requests, and any number of participants. Clients and
participants run as separate processes and talk to the coordinator over
`ipc-channel`.

## Building and running

```
cargo build
./target/debug/two_phase_commit -s .95 -c 4 -p 10 -r 10 -m run
./target/debug/two_phase_commit -s .95 -c 4 -p 10 -r 10 -m check
```

`run` mode writes logs to `./logs/`. `check` mode reads those logs and
verifies that every participant agrees with the coordinator. Run `check` with
the same `-c`, `-p` and `-r` values as the run.

| Flag | Meaning | Default |
|------|---------|---------|
| `-c` | Number of clients | 3 |
| `-p` | Number of participants | 3 |
| `-r` | Requests per client | 15 |
| `-s` | Probability a participant's operation succeeds | 1.0 |
| `-S` | Probability a participant's vote is delivered | 1.0 |
| `-f` | Probability a participant crashes right after voting | 0.0 |
| `-C` | Probability the coordinator crashes at each crash point | 0.0 |
| `-v` | Log verbosity, 0 to 5 | 0 |

`--ipc_path`, `--num` and `--recover` are set by the coordinator when it
starts child processes; you don't pass them yourself.

## Steps 1 and 2: the protocol

For each client request, the coordinator:

1. Logs the proposal, then sends it to every participant.
2. Collects votes for up to one second. Each participant performs the
   operation (succeeding with probability `-s`), logs its vote, then sends it.
   A vote can be lost in transit (delivered with probability `-S`).
3. Decides commit only if every participant voted commit in time. Any abort
   vote, missing vote or disconnected participant means abort.
4. Logs the decision, then sends it to every participant and to the client.

Both sides write to their log before sending (write-ahead logging), so a
log never claims less than what other processes were told.

## Extra credit 1: fault-tolerant log

`oplog.rs` is hardened so the log survives crashes:

- **Single write per entry.** Each entry is serialized to one buffer and
  written with one `write_all`. Writing directly with `serde_json::to_writer`
  issues many small writes, so a crash could leave half an entry on disk.
- **Synced to disk.** `sync_all()` runs after every append, so an entry
  survives a power loss, not just a process crash.
- **Torn final line tolerated.** `from_file` stops at a partially written
  last line instead of panicking.
- **Reopened for appending.** A log loaded with `from_file` can keep growing,
  which recovery depends on.
- **No uid reuse after restart.** Loading a log seeds the message uid counter
  past the highest uid in the file, so a restarted process doesn't overwrite
  its own history.

To see torn-line handling, append a partial entry to a log and run the
checker:

```
./target/debug/two_phase_commit -c 1 -p 1 -r 1 -m run
printf '{"mtype":"CoordinatorCom' >> logs/participant_0.log
./target/debug/two_phase_commit -c 1 -p 1 -r 1 -m check
```

The checker ignores the broken line and passes.

This version hardens the project's own JSON log rather than adopting the
`commitlog` crate, keeping the `OpLog` API the checker relies on.

## Extra credit 2: participant recovery and rejoin

With `-f`, a participant exits right after logging and sending its vote. That
is the moment it is *in doubt*: it has promised to commit if asked, but
doesn't know the outcome.

```
./target/debug/two_phase_commit -f 0.2 -s .9 -S .9 -c 3 -p 3 -r 5 -m run
./target/debug/two_phase_commit -c 3 -p 3 -r 5 -m check
```

**Detection.** The coordinator marks a participant dead when a send to it
fails, when its channel disconnects during voting, or when a check at the
start of the next transaction finds the channel closed.

**Rejoin.** Before proposing the next transaction, the coordinator restarts
each dead participant with the same options plus `--recover`, and registers
its new channels. An `IpcOneShotServer` accepts only one connection, so the
coordinator, which owns the spawning logic, is the natural place to restart
participants.

**Recovery.** The restarted participant reopens its log and:

1. Counts the decisions already logged, so its final report covers the
   whole run.
2. Finds each transaction with a logged vote but no logged decision.
3. Sends `ParticipantRecoveryQuery` for each one. The coordinator answers
   from its log with `CoordinatorCommit` or `CoordinatorAbort`. If the
   coordinator has no decision recorded, the answer is abort (presumed
   abort), since nothing can have committed without a logged decision.
4. Logs each answer, then sends `ParticipantRejoin` and resumes normal
   operation.

A participant that voted abort could safely abort on its own, but asking the
coordinator handles both cases the same way. A participant never votes twice
on the same transaction, which the checker requires.

If a participant crashes during the final transaction, the coordinator
restarts it once more before shutting down, so it still recovers.

Recovery messages bypass the lossy send used for votes: `-S` simulates lost
votes, not a broken recovery handshake.

## Extra credit 3: coordinator failure

With `-C`, the coordinator crashes at one of three points in a transaction:

1. **Votes collected, no decision logged.** Nobody can have been told to
   commit.
2. **Decision logged, not yet sent.** The decision exists only in the log.
3. **Decision sent to some participants but not others.**

A crash discards everything the coordinator holds in memory about the
current transaction. Recovery then rebuilds its state from the log on disk:

- It reloads the log with `OpLog::from_file` and recomputes its commit and
  abort counts.
- It finds the most recent proposal. The coordinator runs one transaction at
  a time, so only that one can be unfinished.
- If that proposal has a logged decision, it resends the decision to every
  participant and to the client. Participants ignore a decision for a
  transaction they aren't waiting on, so resending is safe.
- If it has no logged decision, the coordinator logs an abort (presumed
  abort), then sends it.

Logging each proposal before sending it (a `CoordinatorPropose` record) is
what makes the first case recoverable. Without it, a restarted coordinator
wouldn't know which transaction its participants were waiting on. The checker
counts only commit and abort records, so the extra records don't affect it.

```
./target/debug/two_phase_commit -C 0.2 -c 3 -p 3 -r 5 -m run
./target/debug/two_phase_commit -c 3 -p 3 -r 5 -m check
```

Both kinds of failure can be combined:

```
./target/debug/two_phase_commit -C 0.2 -f 0.2 -s .9 -S .9 -c 3 -p 3 -r 5 -m run
./target/debug/two_phase_commit -c 3 -p 3 -r 5 -m check
```

### Limitations

- **The crash is simulated inside the coordinator process.** The coordinator
  keeps its IPC channels across the crash, standing in for a restarted
  coordinator reconnecting to the same clients and participants. A real
  process restart would need a reconnection mechanism that the one-shot IPC
  setup doesn't provide.
- **2PC blocks while the coordinator is down.** Participants that voted
  commit must wait for the coordinator to come back; they can't safely decide
  on their own. Avoiding that wait is what three-phase commit is for.
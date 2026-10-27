use std::ops::ControlFlow;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread::JoinHandle;
use std::{io, thread};

use eyre::{eyre, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use malachitebft_core_types::{Context, Height};
use malachitebft_wal as wal;

use crate::node::{NodeMsg, NodeRef};
use malachitebft_core_consensus::Input;

use super::entry::{decode_entry, encode_entry, WalCodec};
use super::iter::log_entries;

pub type ReplyTo<T> = oneshot::Sender<Result<T>>;

pub enum WalMsg<Ctx: Context> {
    StartedHeight(Ctx::Height, ReplyTo<Vec<io::Result<Input<Ctx>>>>),
    Reset(Ctx::Height, ReplyTo<()>),
    Append(Input<Ctx>, ReplyTo<()>),
    Flush(ReplyTo<()>),
    Shutdown,
    Dump,
}

pub fn spawn<Ctx, Codec>(
    span: tracing::Span,
    log: wal::Log,
    codec: Codec,
    rx: mpsc::Receiver<WalMsg<Ctx>>,
    node: NodeRef,
) -> JoinHandle<Result<(), String>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    spawn_with_final_flush(span, log, codec, rx, node, wal::Log::flush)
}

fn spawn_with_final_flush<Ctx, Codec, Flush>(
    span: tracing::Span,
    mut log: wal::Log,
    codec: Codec,
    mut rx: mpsc::Receiver<WalMsg<Ctx>>,
    node: NodeRef,
    final_flush: Flush,
) -> JoinHandle<Result<(), String>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
    Flush: FnOnce(&mut wal::Log) -> io::Result<()> + Send + 'static,
{
    thread::spawn(move || {
        let mut first_failure = None;
        let result = catch_unwind(AssertUnwindSafe(|| {
            while let Some(msg) = rx.blocking_recv() {
                match process_msg(msg, &span, &mut log, &codec) {
                    Ok(ControlFlow::Continue(())) => continue,
                    Ok(ControlFlow::Break(())) => break,
                    Err(e) => {
                        error!("WAL task failed: {e}");
                        first_failure.get_or_insert_with(|| e.to_string());
                    }
                }
            }

            info!("WAL thread exiting");

            let result = final_flush(&mut log)
                .map_err(|error| format!("WAL shutdown flush failed: {error}"));
            drop(log);
            result
        }));

        let result = result.unwrap_or_else(|error| {
            Err(format!(
                "WAL worker thread panicked: {}",
                format_panic_reason(error.as_ref())
            ))
        });
        // Operation errors already travel to consensus and remain in the join
        // outcome. Only a terminal worker fault needs an additional notification,
        // preserving the unmanaged path's behavior after an earlier operation error.
        if let Err(reason) = &result {
            let _ = node.cast(NodeMsg::SafetyFailure(reason.clone()));
        }
        let result = match (first_failure, result) {
            (None, result) => result,
            (Some(reason), Ok(())) => Err(reason),
            (Some(reason), Err(final_error)) => Err(format!("{reason}; {final_error}")),
        };
        if let Err(reason) = &result {
            error!(%reason, "WAL worker failed");
        }
        result
    })
}

fn format_panic_reason(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[tracing::instrument(
    name = "wal",
    parent = span,
    skip_all,
    fields(height = span_sequence(log.sequence(), &msg))
)]
fn process_msg<Ctx, Codec>(
    msg: WalMsg<Ctx>,
    span: &tracing::Span,
    log: &mut wal::Log,
    codec: &Codec,
) -> Result<ControlFlow<()>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    match msg {
        WalMsg::StartedHeight(height, reply) => {
            let sequence = height.as_u64();

            if sequence == log.sequence() {
                // WAL is already at that sequence
                // Let's check if there are any entries to replay
                let entries = fetch_entries(log, codec);

                let replay_failure = entries.as_ref().ok().and_then(|entries| {
                    entries.iter().find_map(|entry| {
                        entry
                            .as_ref()
                            .err()
                            .map(|error| eyre!("WAL replay: {error}"))
                    })
                });
                send_reply(reply, entries, "WAL replay")?;
                if let Some(error) = replay_failure {
                    return Err(error);
                }
            } else {
                // WAL is at different sequence, restart it
                // No entries to replay
                let result = log.reset(sequence).map(|_| Vec::new()).map_err(Into::into);

                debug!(%height, "Reset WAL");

                send_reply(reply, result, "WAL reset")?;
            }
        }

        WalMsg::Reset(height, reply) => {
            let sequence = height.as_u64();

            let result = log.reset(sequence).map_err(Into::into);

            debug!(%height, "Reset WAL");

            send_reply(reply, result, "WAL reset")?;
        }

        WalMsg::Append(entry, reply) => {
            let entry_type = wal_entry_type(&entry);

            let mut buf = Vec::new();

            // Capture encoding result and always send a reply to prevent deadlock
            let result = encode_entry(entry, codec, &mut buf)
                .and_then(|_| {
                    if !buf.is_empty() {
                        log.append(&buf)
                    } else {
                        Ok(())
                    }
                })
                .map_err(Into::into);

            if let Err(e) = &result {
                error!("ATTENTION: Failed to append entry to WAL: {e}");
            } else if !buf.is_empty() {
                debug!(
                    type = %entry_type, entry.size = %buf.len(), log.entries = %log.len(),
                    "Wrote log entry"
                );
            }

            send_reply(reply, result, "WAL append")?;
        }

        WalMsg::Flush(reply) => {
            let result = log.flush().map_err(Into::into);

            if let Err(e) = &result {
                error!("ATTENTION: Failed to flush WAL to disk: {e}");
            } else {
                debug!(
                    wal.entries = %log.len(),
                    wal.size = %log.size_bytes().unwrap_or(0),
                    "Flushed WAL to disk"
                );
            }

            send_reply(reply, result, "WAL flush")?;
        }

        WalMsg::Dump => {
            if let Err(e) = dump_entries(log, codec) {
                error!("Failed to dump WAL: {e}");
            }
        }

        WalMsg::Shutdown => {
            info!("Shutting down WAL thread");
            return Ok(ControlFlow::Break(()));
        }
    }

    Ok(ControlFlow::Continue(()))
}

fn send_reply<T>(reply: ReplyTo<T>, result: Result<T>, operation: &str) -> Result<()> {
    // The waiting actor may have been killed during shutdown. Keep operation
    // failures independently of reply delivery so a later successful fsync
    // cannot turn failed persistence or replay into a clean worker join.
    let failure = result
        .as_ref()
        .err()
        .map(|error| eyre!("{operation}: {error}"));
    if reply.send(result).is_err() {
        error!(%operation, "Failed to send WAL reply");
    }
    failure.map_or(Ok(()), Err)
}

fn fetch_entries<Ctx, Codec>(
    log: &mut wal::Log,
    codec: &Codec,
) -> Result<Vec<io::Result<Input<Ctx>>>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    if log.is_empty() {
        return Ok(Vec::new());
    }

    let iter = log
        .iter()
        .map_err(|e| eyre!("Failed to open WAL for reading entries: {e}"))?;

    let mut entries = Vec::new();

    for (idx, result) in iter.enumerate() {
        match result {
            Ok(bytes) => {
                let decoded = decode_result(idx, Ok(bytes), codec);
                entries.push(decoded);
            }
            Err(e) => {
                error!("Failed to read WAL entry {idx}: {e}");
                entries.push(Err(e));

                log.truncate(idx as u64).map_err(|e| {
                    eyre!("Failed to truncate WAL after read error at entry {idx}: {e}")
                })?;

                break;
            }
        }
    }

    Ok(entries)
}

fn decode_result<Ctx, Codec>(
    idx: usize,
    result: io::Result<Vec<u8>>,
    codec: &Codec,
) -> io::Result<Input<Ctx>>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    result
        .inspect_err(|e| error!("Failed to retrieve WAL entry {idx}: {e}"))
        .and_then(|bytes| {
            decode_entry(codec, io::Cursor::new(&bytes)).inspect_err(|e| {
                error!(
                    "Failed to decode WAL entry {idx}: {e} (0x{})",
                    hex::encode(&bytes)
                );
            })
        })
}

fn dump_entries<'a, Ctx, Codec>(log: &'a mut wal::Log, codec: &'a Codec) -> Result<()>
where
    Ctx: Context,
    Codec: WalCodec<Ctx>,
{
    let len = log.len();
    let mut count = 0;

    info!("WAL Dump");
    info!("- Entries: {len}");
    info!("- Size:    {} bytes", log.size_bytes().unwrap_or(0));
    info!("Entries:");

    for (idx, entry) in log_entries(log, codec)?.enumerate() {
        count += 1;

        match entry {
            Ok(entry) => {
                info!("- #{idx}: {entry:?}");
            }
            Err(e) => {
                error!("- #{idx}: Error decoding WAL entry: {e}");
            }
        }
    }

    if count != len {
        error!("Expected {len} entries, but found {count} entries");
    }

    Ok(())
}

fn span_sequence(sequence: u64, msg: &WalMsg<impl Context>) -> u64 {
    if let WalMsg::StartedHeight(height, _) = msg {
        height.as_u64()
    } else {
        sequence
    }
}

fn wal_entry_type<Ctx: Context>(entry: &Input<Ctx>) -> &'static str {
    match entry {
        Input::Vote(_) => "Consensus(Vote)",
        Input::Proposal(_) => "Consensus(Proposal)",
        Input::ProposedValue(_, _) => "ProposedValue",
        Input::TimeoutElapsed(_) => "Timeout",
        Input::PolkaCertificate(_) => "PolkaCertificate",
        Input::StartHeight(..)
        | Input::Propose(..)
        | Input::RoundCertificate(..)
        | Input::SyncValueResponse(..) => "Unpersisted",
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::io;

    use bytes::Bytes;
    use malachitebft_codec::Codec;
    use malachitebft_core_consensus::{Input, ProposedValue};
    use malachitebft_core_types::{Round, Validity, ValueOrigin};
    use malachitebft_metrics::{Metrics, SharedRegistry};
    use malachitebft_test::codec::proto::ProtobufCodec;
    use malachitebft_test::{Height, PrivateKey, TestContext, Validator, Value};
    use malachitebft_wal::Log;
    use tempfile::tempdir;
    use tokio::sync::{mpsc, oneshot};
    use tracing::Span;

    use crate::node::{Node, NodeMonitor, NodeMsg};
    use crate::wal::WorkerRegistry;

    use super::{spawn_with_final_flush, WalMsg};

    struct FailingCodec;

    impl<T> Codec<T> for FailingCodec {
        type Error = io::Error;

        fn decode(&self, _: Bytes) -> io::Result<T> {
            Err(io::Error::other("injected decode failure"))
        }

        fn encode(&self, _: &T) -> io::Result<Bytes> {
            Err(io::Error::other("injected append encoding failure"))
        }
    }

    #[tokio::test]
    async fn operation_and_terminal_failures_have_distinct_notifications() {
        for final_failure in [false, true] {
            let directory = tempdir().unwrap();
            let path = directory
                .path()
                .join(format!("failed-append-{final_failure}.wal"));
            let registry = SharedRegistry::new(Default::default(), None);
            let metrics = Metrics::register(&registry);
            let monitor = NodeMonitor::default();
            let (node, node_join) = Node::new(metrics, Span::none())
                .with_monitor(monitor.clone())
                .spawn()
                .await
                .unwrap();
            let (commands, receiver) = mpsc::channel::<WalMsg<TestContext>>(2);
            let worker = spawn_with_final_flush(
                Span::none(),
                Log::open(&path).unwrap(),
                FailingCodec,
                receiver,
                node.clone(),
                move |log| {
                    if final_failure {
                        Err(io::Error::other("injected final fsync failure"))
                    } else {
                        log.flush()
                    }
                },
            );
            let validator = Validator::new(PrivateKey::from([9; 32]).public_key(), 1);
            let value = ProposedValue {
                height: Height::new(1),
                round: Round::new(0),
                valid_round: Round::Nil,
                proposer: validator.address,
                value: Value::new(42),
                validity: Validity::Valid,
            };
            let (reply, receiver) = oneshot::channel();
            drop(receiver);
            commands
                .send(WalMsg::Append(
                    Input::ProposedValue(value, ValueOrigin::Consensus),
                    reply,
                ))
                .await
                .unwrap();
            commands.send(WalMsg::Shutdown).await.unwrap();
            drop(commands);
            let error = worker.join().unwrap().unwrap_err();
            assert!(error.contains("injected append encoding failure"));
            assert_eq!(
                error.contains("injected final fsync failure"),
                final_failure
            );
            ractor::call!(node, NodeMsg::PrepareShutdown).unwrap();
            assert_eq!(monitor.failure().is_some(), final_failure);
            drop(Log::open(&path).expect("joined worker must release the WAL lock"));
            node.stop(None);
            node_join.await.unwrap();
        }
    }

    #[tokio::test]
    async fn final_flush_failure_is_joined_and_releases_the_file_lock() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shutdown.wal");
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, node_join) = Node::new(metrics, Span::none()).spawn().await.unwrap();
        let (commands, receiver) = mpsc::channel::<WalMsg<TestContext>>(1);
        let worker = spawn_with_final_flush(
            Span::none(),
            Log::open(&path).unwrap(),
            ProtobufCodec,
            receiver,
            node.clone(),
            |_log| Err(io::Error::other("injected final fsync failure")),
        );
        let workers = WorkerRegistry::default();
        workers.handles.lock().unwrap().push(worker);
        commands.send(WalMsg::Shutdown).await.unwrap();
        drop(commands);
        let errors = workers.join().unwrap_err();
        assert_eq!(
            errors,
            ["WAL shutdown flush failed: injected final fsync failure"]
        );
        drop(Log::open(&path).expect("joined worker must have released its advisory lock"));
        node.stop(None);
        node_join.await.unwrap();
    }
}

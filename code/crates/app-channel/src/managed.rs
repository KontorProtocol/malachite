use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use futures::FutureExt;
use malachitebft_engine::node::{NodeMonitor, NodeMsg, NodeRef};
use malachitebft_engine::wal::WorkerRegistry;
use ractor::{ActorCell, ActorStatus};
use thiserror::Error;
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

use crate::app::metrics::SharedRegistry;
use crate::app::types::core::Context;
use crate::{Channels, EngineHandle};

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ManagedEngineError {
    #[error("engine startup failed: {0}")]
    Startup(String),
    #[error("engine stopped unexpectedly: {0}")]
    UnexpectedExit(String),
    #[error("engine thread panicked")]
    Panic,
    #[error("engine supervision failed: {0}")]
    Supervision(String),
    #[error("WAL shutdown failed: {0:?}")]
    Wal(Vec<String>),
    #[error("{engine}; WAL shutdown also failed: {wal:?}")]
    WithWal {
        engine: Box<ManagedEngineError>,
        wal: Vec<String>,
    },
    #[error("engine startup channels are no longer available")]
    ChannelsUnavailable,
}

type Outcome = Result<(), ManagedEngineError>;
type Completion = Arc<(Mutex<Option<Outcome>>, Condvar)>;

#[derive(Clone, Default)]
pub(crate) struct ManagedActors(Arc<Mutex<ActorOwnership>>);

#[derive(Default)]
struct ActorOwnership {
    node: Option<NodeRef>,
    children: Vec<ActorCell>,
}

impl ManagedActors {
    pub(crate) fn node(&self, node: NodeRef) {
        self.0.lock().unwrap().node = Some(node);
    }

    pub(crate) fn child(&self, child: ActorCell) {
        self.0.lock().unwrap().children.push(child);
    }

    async fn shutdown(&self) -> Outcome {
        let ActorOwnership { node, children } = std::mem::take(&mut *self.0.lock().unwrap());
        let mut outcome = Ok(());
        if let Some(node) = &node {
            if node.get_status() != ActorStatus::Stopped {
                outcome = ractor::call!(node, NodeMsg::PrepareShutdown)
                    .map_err(|error| ManagedEngineError::UnexpectedExit(error.to_string()));
            }
        }
        // Registration follows construction dependencies. A dependent may still
        // finish an in-flight RPC after receiving stop, so its providers must
        // remain available until it reaches Stopped, including after a kill.
        for child in children.iter().rev() {
            child.stop(Some("managed engine shutdown".into()));
            if tokio::time::timeout(Duration::from_secs(1), wait_for_actor(child))
                .await
                .is_err()
            {
                child.kill();
                wait_for_actor(child).await;
            }
        }
        if let Some(node) = node {
            if node.get_status() != ActorStatus::Stopped {
                // Terminal supervision is queued before the child becomes stopped.
                // Drain it before removing the supervisor, including shutdown faults.
                let drained = ractor::call!(node, NodeMsg::PrepareShutdown)
                    .map_err(|error| ManagedEngineError::UnexpectedExit(error.to_string()));
                outcome = outcome.and(drained);
                node.stop(Some("managed engine shutdown".into()));
                wait_for_actor(&node.get_cell()).await;
            }
        }
        outcome
    }
}

/// Owns an engine runtime and its WAL workers. A timeout never relinquishes the
/// thread handle. Dropping an unjoined owner requests stop and blocks until every
/// worker exits, so keep this owner on a control thread rather than a runtime worker.
#[must_use]
pub struct ManagedEngine<Ctx: Context> {
    ready: Option<mpsc::Receiver<Channels<Ctx>>>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    completion: Completion,
    outcome: Option<Outcome>,
    registry: SharedRegistry,
}

impl<Ctx: Context> ManagedEngine<Ctx> {
    pub(crate) fn spawn<F, Fut>(registry: SharedRegistry, build: F) -> eyre::Result<Self>
    where
        F: FnOnce(SharedRegistry, WorkerRegistry, NodeMonitor, ManagedActors) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = eyre::Result<(Channels<Ctx>, EngineHandle)>>,
    {
        let (ready_tx, ready) = mpsc::channel();
        let (stop, stop_rx) = oneshot::channel();
        let completion: Completion = Arc::new((Mutex::new(None), Condvar::new()));
        let worker_completion = Arc::clone(&completion);
        let worker_registry = registry.clone();
        let thread = thread::Builder::new()
            .name("malachite-engine".into())
            .spawn(move || {
                let workers = WorkerRegistry::default();
                let monitor = NodeMonitor::default();
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    run_engine(
                        build,
                        worker_registry,
                        workers.clone(),
                        monitor.clone(),
                        ready_tx,
                        stop_rx,
                    )
                }))
                .unwrap_or(Err(ManagedEngineError::Panic));
                let outcome = match monitor.failure() {
                    Some(mut reason) => {
                        if let Err(error) = outcome {
                            reason.push_str(&format!("; {error}"));
                        }
                        Err(ManagedEngineError::Supervision(reason))
                    }
                    None => outcome,
                };
                // Runtime destruction closes all actor-owned WAL senders. Joining
                // here also covers actor failure, kill and partial startup failure.
                let outcome = match (outcome, workers.join()) {
                    (outcome, Ok(())) => outcome,
                    (Ok(()), Err(wal)) => Err(ManagedEngineError::Wal(wal)),
                    (Err(engine), Err(wal)) => Err(ManagedEngineError::WithWal {
                        engine: Box::new(engine),
                        wal,
                    }),
                };
                *worker_completion.0.lock().unwrap() = Some(outcome);
                worker_completion.1.notify_all();
            })?;
        Ok(Self {
            ready: Some(ready),
            stop: Some(stop),
            thread: Some(thread),
            completion,
            outcome: None,
            registry,
        })
    }

    /// Returns None on timeout, retaining ownership for another wait or shutdown.
    /// Build failures are returned only after complete teardown and thread join.
    /// ChannelsUnavailable reports consumed or canceled readiness, not teardown.
    pub fn wait_ready(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Channels<Ctx>>, ManagedEngineError> {
        let started = Instant::now();
        let receiver = self
            .ready
            .as_ref()
            .ok_or(ManagedEngineError::ChannelsUnavailable)?;
        match receiver.recv_timeout(timeout) {
            Ok(channels) => {
                self.ready = None;
                Ok(Some(channels))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if self.wait_stopped(timeout.saturating_sub(started.elapsed()))? {
                    Err(ManagedEngineError::UnexpectedExit(
                        "engine ended without delivering channels".into(),
                    ))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn registry(&self) -> &SharedRegistry {
        &self.registry
    }

    pub fn request_stop(&mut self) {
        self.ready = None;
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    /// Returns false on timeout. Only a joined engine can return true or a fault.
    /// A timed-out caller must keep this owner and must not start another engine.
    pub fn wait_stopped(&mut self, timeout: Duration) -> Result<bool, ManagedEngineError> {
        if let Some(outcome) = &self.outcome {
            return outcome.clone().map(|()| true);
        }
        let started = Instant::now();
        let mut completion = self.completion.0.lock().unwrap();
        while completion.is_none() {
            if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
                break;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Ok(false);
            }
            completion = self
                .completion
                .1
                .wait_timeout(completion, remaining.min(Duration::from_millis(100)))
                .unwrap()
                .0;
        }
        let mut outcome = completion.take().unwrap_or(Err(ManagedEngineError::Panic));
        drop(completion);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                outcome = Err(ManagedEngineError::Panic);
            }
        }
        self.outcome = Some(outcome.clone());
        outcome.map(|()| true)
    }
}

impl<Ctx: Context> Drop for ManagedEngine<Ctx> {
    fn drop(&mut self) {
        self.request_stop();
        if let Some(thread) = self.thread.take() {
            if Handle::try_current().is_ok() {
                tracing::error!(
                    "ManagedEngine dropped inside a Tokio runtime context; join blocks this thread"
                );
            }
            if thread.join().is_err() {
                tracing::error!("Managed engine thread panicked during drop");
            }
        }
    }
}

fn run_engine<Ctx, F, Fut>(
    build: F,
    registry: SharedRegistry,
    workers: WorkerRegistry,
    monitor: NodeMonitor,
    ready: mpsc::Sender<Channels<Ctx>>,
    mut stop: oneshot::Receiver<()>,
) -> Outcome
where
    Ctx: Context,
    F: FnOnce(SharedRegistry, WorkerRegistry, NodeMonitor, ManagedActors) -> Fut,
    Fut: Future<Output = eyre::Result<(Channels<Ctx>, EngineHandle)>>,
{
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("malachite-rt")
        .enable_all()
        .build()
        .map_err(|error| ManagedEngineError::Startup(error.to_string()))?;
    let outcome = runtime.block_on(async {
        let actors = ManagedActors::default();
        // Catch startup panics while the runtime can still drain actors created
        // before the builder failed. Every early return uses the same cleanup.
        let outcome = AssertUnwindSafe(async {
            // Actor::spawn awaits pre_start before returning its handle. Dropping
            // that future midway could orphan resources before we can track them.
            // Finish startup even after a stop request; canceled readiness then
            // routes directly to cleanup. A wedged startup retains this owner.
            let built = build(registry, workers, monitor, actors.clone()).await;
            let (channels, mut engine) =
                built.map_err(|error| ManagedEngineError::Startup(error.to_string()))?;
            if ready.send(channels).is_err() {
                return Ok(());
            }
            tokio::select! {
                biased;
                result = &mut engine.handle => {
                    Err(ManagedEngineError::UnexpectedExit(format!("{result:?}")))
                }
                _ = &mut stop => Ok(())
            }
        })
        .catch_unwind()
        .await
        .unwrap_or(Err(ManagedEngineError::Panic));
        let cleanup = actors.shutdown().await;
        outcome.and(cleanup)
    });
    // No timeout here: Tokio's timed shutdown leaks unfinished blocking work.
    // Runtime destruction cancels pending application replies and owns nested
    // transport tasks. The outer thread then joins the separately owned WAL.
    drop(runtime);
    outcome
}

async fn wait_for_actor(actor: &ActorCell) {
    // ractor 0.15 stores only one permit for waiters arriving after termination.
    // A grace-period wait can consume it, so a second wait can sleep forever.
    while actor.get_status() != ActorStatus::Stopped {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ractor::{async_trait, call, Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::ManagedActors;

    struct Provider;

    #[async_trait]
    impl Actor for Provider {
        type Msg = RpcReplyPort<()>;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            _args: (),
        ) -> Result<(), ActorProcessingErr> {
            Ok(())
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            reply: Self::Msg,
            _state: &mut (),
        ) -> Result<(), ActorProcessingErr> {
            let _ = reply.send(());
            Ok(())
        }
    }

    struct Dependent(ActorRef<RpcReplyPort<()>>);

    #[async_trait]
    impl Actor for Dependent {
        type Msg = ();
        type State = Option<oneshot::Sender<bool>>;
        type Arguments = oneshot::Sender<bool>;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            result: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(Some(result))
        }

        async fn post_stop(
            &self,
            _myself: ActorRef<Self::Msg>,
            result: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            // A provider must outlive its dependent's last RPC. Stop has higher
            // priority than RPC messages, so stopping both first loses this call.
            let available = call!(self.0, |reply| reply).is_ok();
            result.take().unwrap().send(available).unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn shutdown_keeps_providers_alive_until_dependents_finish() {
        let actors = ManagedActors::default();
        let (provider, provider_join) = Actor::spawn(None, Provider, ()).await.unwrap();
        actors.child(provider.get_cell());
        let (result, observed) = oneshot::channel();
        let (dependent, dependent_join) = Actor::spawn(None, Dependent(provider), result)
            .await
            .unwrap();
        actors.child(dependent.get_cell());
        timeout(Duration::from_secs(5), actors.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(
            observed.await.unwrap(),
            "shutdown closed a live RPC provider"
        );
        provider_join.await.unwrap();
        dependent_join.await.unwrap();
    }
}

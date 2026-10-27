use std::collections::BTreeSet;
use std::fs;
use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arc_malachitebft_app_channel::app::config::{ConsensusConfig, NodeConfig, ValueSyncConfig};
use arc_malachitebft_app_channel::app::types::codec::{Codec, WalCodec};
use arc_malachitebft_app_channel::app::types::core::{HeightParams, Round, Validity, ValueOrigin};
use arc_malachitebft_app_channel::app::types::{Keypair, LocallyProposedValue, ProposedValue};
use arc_malachitebft_app_channel::{
    AppMsg, Channels, ConsensusContext, EngineBuilder, ManagedEngine, ManagedEngineError,
    NetworkContext, NetworkIdentity, NetworkRequest, RequestContext, SyncContext, WalContext,
};
use bytes::Bytes;
use if_watch::tokio::IfWatcher;
use malachitebft_core_consensus::Input;
use malachitebft_engine::wal::encode_entry;
use malachitebft_test::codec::{json::JsonCodec, proto::ProtobufCodec};
use malachitebft_test::middleware::Middleware;
use malachitebft_test::{
    Address, Ed25519Signer, Ed25519Verifier, Height, PrivateKey, Proposal, TestContext, Validator,
    ValidatorSet, Value,
};
use malachitebft_wal::Log;
use tokio::runtime::{Builder, Runtime};
use tokio::time::{sleep, timeout};

#[derive(Default)]
struct Config {
    consensus: ConsensusConfig,
    sync: ValueSyncConfig,
    blocking: Option<Arc<Gate>>,
    launched: AtomicBool,
    startup: Option<Arc<Gate>>,
    sync_calls: AtomicUsize,
}

impl NodeConfig for Config {
    fn moniker(&self) -> &str {
        "managed-lifecycle"
    }
    fn consensus(&self) -> &ConsensusConfig {
        if !self.launched.swap(true, Ordering::AcqRel) {
            if let Some(gate) = self.blocking.clone() {
                tokio::task::spawn_blocking(move || gate.enter());
            }
        }
        &self.consensus
    }
    fn consensus_mut(&mut self) -> &mut ConsensusConfig {
        &mut self.consensus
    }
    fn value_sync(&self) -> &ValueSyncConfig {
        if self.sync_calls.fetch_add(1, Ordering::Relaxed) == 1 {
            if let Some(gate) = &self.startup {
                gate.enter();
            }
        }
        &self.sync
    }
    fn value_sync_mut(&mut self) -> &mut ValueSyncConfig {
        &mut self.sync
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("malachite-managed-{}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn start(address: &str, wal: &Path) -> ManagedEngine<TestContext> {
    start_with_codec(address, wal, ProtobufCodec)
}

fn start_with_codec<C: WalCodec<TestContext>>(
    address: &str,
    wal: &Path,
    codec: C,
) -> ManagedEngine<TestContext> {
    start_custom(
        address,
        wal,
        codec,
        TestContext::default(),
        Config::default(),
    )
}

fn start_custom<C: WalCodec<TestContext>>(
    address: &str,
    wal: &Path,
    codec: C,
    ctx: TestContext,
    mut config: Config,
) -> ManagedEngine<TestContext> {
    config.consensus.p2p.listen_addr = address.parse().unwrap();
    let identity = NetworkIdentity::new(
        "managed-lifecycle".into(),
        Keypair::ed25519_from_bytes([7; 32]).unwrap(),
        None,
    );
    let key = PrivateKey::from([9; 32]);
    EngineBuilder::new(ctx, config)
        .with_default_network(NetworkContext::new(identity, JsonCodec))
        .with_default_wal(WalContext::new(wal.into(), codec))
        .with_default_sync(SyncContext::new(JsonCodec))
        .with_default_consensus(ConsensusContext::new_validator(
            Address::from_public_key(&key.public_key()),
            Box::new(Ed25519Verifier),
            Box::new(Ed25519Signer::new(key)),
        ))
        .with_default_request(RequestContext::new(16))
        .start_managed()
        .unwrap()
}

fn bound(runtime: &Runtime, channels: &Channels<TestContext>) {
    runtime.block_on(async {
        timeout(Duration::from_secs(10), async {
            loop {
                let dump = NetworkRequest::dump_state(&channels.net_requests)
                    .await
                    .unwrap()
                    .unwrap();
                if !dump.local_node.listen_addr.to_string().contains("0.0.0.0") {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    });
}

#[cfg(target_os = "linux")]
fn resources() -> (usize, usize) {
    (
        fs::read_dir("/proc/self/task").unwrap().count(),
        fs::read_dir("/proc/self/fd").unwrap().count(),
    )
}

#[cfg(target_os = "macos")]
fn resources() -> (usize, usize) {
    let pid = std::process::id().to_string();
    let threads = Command::new("ps")
        .args(["-M", "-p", &pid])
        .output()
        .unwrap();
    assert!(threads.status.success());
    let files = Command::new("lsof")
        .args(["-nP", "-a", "-p", &pid, "-Ff"])
        .output()
        .unwrap();
    assert!(files.status.success());
    (
        String::from_utf8(threads.stdout)
            .unwrap()
            .lines()
            .skip(1)
            .count(),
        String::from_utf8(files.stdout)
            .unwrap()
            .lines()
            .filter(|line| {
                line.starts_with('f') && line.as_bytes().get(1).is_some_and(u8::is_ascii_digit)
            })
            .count(),
    )
}

fn cycle(runtime: &Runtime, address: &str, wal: &Path, heartbeat: &AtomicU64) -> usize {
    let mut engine = start(address, wal);
    let mut channels = engine.wait_ready(Duration::from_secs(10)).unwrap().unwrap();
    bound(runtime, &channels);
    let pending = runtime
        .block_on(async { timeout(Duration::from_secs(5), channels.consensus.recv()).await })
        .unwrap()
        .unwrap();
    assert!(matches!(&pending, AppMsg::ConsensusReady { .. }));
    let mut metrics = String::new();
    engine.registry().export(&mut metrics).unwrap();
    let families: Vec<_> = metrics
        .lines()
        .filter(|line| line.starts_with("# TYPE "))
        .collect();
    assert_eq!(
        families.iter().copied().collect::<BTreeSet<_>>().len(),
        families.len()
    );
    let count = families.len();
    assert!(count > 0);
    let before = heartbeat.load(Ordering::Relaxed);
    engine.request_stop();
    assert!(engine.wait_stopped(Duration::from_secs(10)).unwrap());
    assert!(heartbeat.load(Ordering::Relaxed) > before);
    assert!(channels.requests.is_closed());
    assert!(channels.net_requests.is_closed());
    assert!(channels.network.is_closed());
    if let AppMsg::ConsensusReady { reply } = pending {
        assert!(reply.is_closed());
    }
    drop(channels);
    drop(engine);
    count
}

#[derive(Debug)]
struct Gate {
    entered: mpsc::Sender<()>,
    released: Mutex<bool>,
    wake: Condvar,
    panic: bool,
}

impl Gate {
    fn enter(&self) {
        self.entered.send(()).unwrap();
        assert!(!self.panic, "injected gated operation panic");
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct ReleaseOnDrop(Arc<Gate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct GatedCodec(Arc<Gate>);

impl<T> Codec<T> for GatedCodec
where
    ProtobufCodec: Codec<T>,
{
    type Error = <ProtobufCodec as Codec<T>>::Error;

    fn decode(&self, bytes: Bytes) -> Result<T, Self::Error> {
        self.0.enter();
        ProtobufCodec.decode(bytes)
    }

    fn encode(&self, value: &T) -> Result<Bytes, Self::Error> {
        ProtobufCodec.encode(value)
    }
}

fn wal_failure_cases(runtime: &Runtime, directory: &Path, heartbeat: &AtomicU64) {
    for panic in [false, true] {
        let baseline = resources();
        let path = directory.join(format!("gated-{panic}.wal"));
        let validator = Validator::new(PrivateKey::from([9; 32]).public_key(), 1);
        let value = ProposedValue::<TestContext> {
            height: Height::new(1),
            round: Round::new(0),
            valid_round: Round::Nil,
            proposer: validator.address,
            value: Value::new(42),
            validity: Validity::Valid,
        };
        let mut encoded = Vec::new();
        encode_entry(
            Input::ProposedValue(value, ValueOrigin::Consensus),
            &ProtobufCodec,
            &mut encoded,
        )
        .unwrap();
        let mut log = Log::open(&path).unwrap();
        log.reset(1).unwrap();
        log.append(encoded).unwrap();
        log.flush().unwrap();
        drop(log);

        let (entered, receiver) = mpsc::channel();
        let gate = Arc::new(Gate {
            entered,
            released: Mutex::new(false),
            wake: Condvar::new(),
            panic,
        });
        let mut engine = start_with_codec("/ip4/0.0.0.0/tcp/0", &path, GatedCodec(gate.clone()));
        // Release before the owner's blocking Drop if any assertion unwinds.
        let release = ReleaseOnDrop(gate);
        let mut channels = engine.wait_ready(Duration::from_secs(10)).unwrap().unwrap();
        bound(runtime, &channels);
        let ready = runtime.block_on(async {
            timeout(Duration::from_secs(5), channels.consensus.recv())
                .await
                .unwrap()
                .unwrap()
        });
        let AppMsg::ConsensusReady { reply } = ready else {
            panic!("expected startup request");
        };
        reply
            .send((
                Height::new(1),
                HeightParams::new(ValidatorSet::new([validator]), Default::default(), None),
            ))
            .unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        engine.request_stop();
        if panic {
            let error = engine.wait_stopped(Duration::from_secs(10)).unwrap_err();
            assert!(matches!(
                error,
                ManagedEngineError::Wal(_) | ManagedEngineError::WithWal { .. }
            ));
            assert!(error.to_string().contains("injected gated operation panic"));
        } else {
            let before = heartbeat.load(Ordering::Relaxed);
            for wait in [Duration::from_secs(2), Duration::from_millis(100)] {
                assert!(!engine.wait_stopped(wait).unwrap());
            }
            assert!(channels.net_requests.is_closed());
            assert!(heartbeat.load(Ordering::Relaxed) > before);
            release.0.release();
            assert!(engine.wait_stopped(Duration::from_secs(10)).unwrap());
        }
        drop(release);
        drop(channels);
        drop(engine);
        assert_eq!(resources(), baseline, "WAL fault/timeout leaked resources");
    }
}

#[derive(Debug)]
struct PanicProposal(Arc<AtomicBool>, Option<Arc<Gate>>);

impl Middleware for PanicProposal {
    fn new_proposal(
        &self,
        _ctx: &TestContext,
        _height: Height,
        _round: Round,
        _value: Value,
        _pol_round: Round,
        _address: Address,
    ) -> Proposal {
        if let Some(gate) = &self.1 {
            gate.enter();
        }
        self.0.store(true, Ordering::Release);
        panic!("injected consensus actor panic");
    }
}

fn actor_failure(runtime: &Runtime, directory: &Path, during_shutdown: bool) {
    let baseline = resources();
    let panicked = Arc::new(AtomicBool::new(false));
    let (entered, receiver) = mpsc::channel();
    let gate = during_shutdown.then(|| {
        Arc::new(Gate {
            entered,
            released: Mutex::new(false),
            wake: Condvar::new(),
            panic: false,
        })
    });
    let ctx = TestContext::with_middleware(Arc::new(PanicProposal(panicked.clone(), gate.clone())));
    let mut engine = start_custom(
        "/ip4/0.0.0.0/tcp/0",
        &directory.join(format!("actor-panic-{during_shutdown}.wal")),
        ProtobufCodec,
        ctx,
        Config::default(),
    );
    let release = gate.map(ReleaseOnDrop);
    let mut channels = engine.wait_ready(Duration::from_secs(10)).unwrap().unwrap();
    bound(runtime, &channels);
    runtime.block_on(async {
        timeout(Duration::from_secs(10), async {
            while let Some(message) = channels.consensus.recv().await {
                match message {
                    AppMsg::ConsensusReady { reply } => {
                        let validator = Validator::new(PrivateKey::from([9; 32]).public_key(), 1);
                        reply
                            .send((
                                Height::new(1),
                                HeightParams::new(
                                    ValidatorSet::new([validator]),
                                    Default::default(),
                                    None,
                                ),
                            ))
                            .unwrap();
                    }
                    AppMsg::StartedRound { reply_value, .. } => {
                        reply_value.send(Vec::new()).unwrap()
                    }
                    AppMsg::GetValue {
                        height,
                        round,
                        reply,
                        ..
                    } => {
                        reply
                            .send(LocallyProposedValue::new(height, round, Value::new(42)))
                            .unwrap();
                        return;
                    }
                    other => panic!("unexpected startup message: {other:?}"),
                }
            }
            panic!("engine closed before proposal request");
        })
        .await
        .unwrap();
    });
    if let Some(release) = &release {
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        engine.request_stop();
        assert!(!engine.wait_stopped(Duration::from_secs(2)).unwrap());
        release.0.release();
    }
    let error = engine.wait_stopped(Duration::from_secs(10)).unwrap_err();
    assert!(
        matches!(error, ManagedEngineError::Supervision(_)),
        "{error}"
    );
    assert!(error.to_string().contains("injected consensus actor panic"));
    assert!(panicked.load(Ordering::Acquire));
    drop(release);
    drop(channels);
    drop(engine);
    assert_eq!(resources(), baseline, "actor failure leaked resources");
}

fn blocking_runtime_work(runtime: &Runtime, directory: &Path, heartbeat: &AtomicU64) {
    let baseline = resources();
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new(Gate {
        entered,
        released: Mutex::new(false),
        wake: Condvar::new(),
        panic: false,
    });
    let config = Config {
        blocking: Some(gate.clone()),
        ..Config::default()
    };
    let mut engine = start_custom(
        "/ip4/0.0.0.0/tcp/0",
        &directory.join("blocking.wal"),
        ProtobufCodec,
        TestContext::default(),
        config,
    );
    let release = ReleaseOnDrop(gate);
    let channels = engine.wait_ready(Duration::from_secs(10)).unwrap().unwrap();
    bound(runtime, &channels);
    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    let before = heartbeat.load(Ordering::Relaxed);
    engine.request_stop();
    assert!(!engine.wait_stopped(Duration::from_secs(2)).unwrap());
    assert!(channels.net_requests.is_closed());
    assert!(heartbeat.load(Ordering::Relaxed) > before);
    release.0.release();
    assert!(engine.wait_stopped(Duration::from_secs(10)).unwrap());
    drop(release);
    drop(channels);
    drop(engine);
    assert_eq!(resources(), baseline, "Tokio blocking worker outlived join");
}

fn quiet_interface_watchers(runtime: &Runtime) {
    let baseline = resources();
    let mut worst_drop = Duration::ZERO;
    for settle in [Duration::ZERO, Duration::from_millis(150)] {
        for _ in 0..10 {
            let watcher = runtime.block_on(async { IfWatcher::new().unwrap() });
            thread::sleep(settle);
            #[cfg(target_os = "macos")]
            if !settle.is_zero() {
                assert_eq!(
                    resources().0,
                    baseline.0 + 1,
                    "watcher worker failed to start"
                );
            }
            #[cfg(target_os = "linux")]
            assert!(
                resources().1 > baseline.1,
                "watcher did not open its netlink socket"
            );
            let started = Instant::now();
            drop(watcher);
            let elapsed = started.elapsed();
            worst_drop = worst_drop.max(elapsed);
            assert!(
                elapsed < Duration::from_secs(2),
                "quiet watcher drop took {elapsed:?}"
            );
            assert_eq!(resources(), baseline, "quiet watcher leaked resources");
        }
    }
    println!("20 quiet interface watcher drops, worst latency {worst_drop:?}");
}

fn cancel_partial_startup(directory: &Path, heartbeat: &AtomicU64) {
    let baseline = resources();
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new(Gate {
        entered,
        released: Mutex::new(false),
        wake: Condvar::new(),
        panic: false,
    });
    let config = Config {
        startup: Some(gate.clone()),
        ..Config::default()
    };
    let mut engine = start_custom(
        "/ip4/0.0.0.0/tcp/0",
        &directory.join("cancel-startup.wal"),
        ProtobufCodec,
        TestContext::default(),
        config,
    );
    let release = ReleaseOnDrop(gate);
    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(engine
        .wait_ready(Duration::from_millis(10))
        .unwrap()
        .is_none());
    let before = heartbeat.load(Ordering::Relaxed);
    engine.request_stop();
    assert!(matches!(
        engine.wait_ready(Duration::ZERO),
        Err(ManagedEngineError::ChannelsUnavailable)
    ));
    assert!(!engine.wait_stopped(Duration::from_secs(2)).unwrap());
    assert!(heartbeat.load(Ordering::Relaxed) > before);
    release.0.release();
    assert!(engine.wait_stopped(Duration::from_secs(10)).unwrap());
    drop(release);
    drop(engine);
    assert_eq!(
        resources(),
        baseline,
        "canceled partial startup leaked resources"
    );
}

fn panic_partial_startup(directory: &Path) {
    let baseline = resources();
    let (entered, receiver) = mpsc::channel();
    let config = Config {
        startup: Some(Arc::new(Gate {
            entered,
            released: Mutex::new(true),
            wake: Condvar::new(),
            panic: true,
        })),
        ..Config::default()
    };
    let mut engine = start_custom(
        "/ip4/0.0.0.0/tcp/0",
        &directory.join("panic-startup.wal"),
        ProtobufCodec,
        TestContext::default(),
        config,
    );
    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        engine.wait_stopped(Duration::from_secs(10)),
        Err(ManagedEngineError::Panic)
    );
    drop(engine);
    assert_eq!(resources(), baseline, "startup panic leaked resources");
}

#[test]
fn real_engines_release_ports_workers_metrics_and_pending_replies() {
    let directory = Directory::new();
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let heartbeat = Arc::new(AtomicU64::new(0));
    let tick = Arc::clone(&heartbeat);
    runtime.spawn(async move {
        loop {
            tick.fetch_add(1, Ordering::Relaxed);
            sleep(Duration::from_millis(5)).await;
        }
    });
    for quic in [false, true] {
        let port = if quic {
            UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        } else {
            TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let address = if quic {
            format!("/ip4/0.0.0.0/udp/{port}/quic-v1")
        } else {
            format!("/ip4/0.0.0.0/tcp/{port}")
        };
        let wal = directory.0.join(if quic { "quic.wal" } else { "tcp.wal" });
        let families = cycle(&runtime, &address, &wal, &heartbeat);
        let baseline = resources();
        for iteration in 0..20 {
            assert_eq!(cycle(&runtime, &address, &wal, &heartbeat), families);
            assert_eq!(
                resources(),
                baseline,
                "resource growth in cycle {iteration}, quic={quic}"
            );
            if quic {
                drop(UdpSocket::bind(("0.0.0.0", port)).unwrap());
            } else {
                drop(TcpListener::bind(("0.0.0.0", port)).unwrap());
            }
        }
        let mut failed = start(&address, &directory.0);
        assert!(matches!(
            failed.wait_ready(Duration::from_secs(10)),
            Err(ManagedEngineError::Startup(_))
        ));
        drop(failed);
        assert_eq!(resources(), baseline, "partial startup leaked resources");
    }
    wal_failure_cases(&runtime, &directory.0, &heartbeat);
    for during_shutdown in [false, true] {
        actor_failure(&runtime, &directory.0, during_shutdown);
    }
    blocking_runtime_work(&runtime, &directory.0, &heartbeat);
    quiet_interface_watchers(&runtime);
    cancel_partial_startup(&directory.0, &heartbeat);
    panic_partial_startup(&directory.0);
}

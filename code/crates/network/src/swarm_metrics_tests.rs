use std::ops::ControlFlow;
use std::time::Duration;

use libp2p::core::ConnectedPoint;
use libp2p::identify;
use libp2p::identity::Keypair;
use libp2p::metrics::Metrics;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{noise, tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder};
use malachitebft_metrics::prometheus::encoding::text::encode;
use malachitebft_metrics::Registry;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::behaviour::{Behaviour, NetworkEvent};
use crate::state::tests::test_state;
use crate::state::State;
use crate::tests::gossip_test_config;
use crate::{handle_swarm_event, Config, Event, NetworkIdentity};

struct Fixture {
    config: Config,
    registry: Registry,
    metrics: Metrics,
    swarm: Swarm<Behaviour>,
    state: State,
    sender: mpsc::Sender<Event>,
    receiver: mpsc::Receiver<Event>,
}

impl Fixture {
    fn new() -> Self {
        let config = gossip_test_config();
        let identity =
            NetworkIdentity::new("metrics-test".into(), Keypair::generate_ed25519(), None);
        let mut registry = Registry::default();
        let metrics = Metrics::new(&mut registry);
        let behaviour = Behaviour::new_with_metrics(&config, &identity, &mut registry).unwrap();
        let swarm = SwarmBuilder::with_existing_identity(identity.keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| behaviour)
            .unwrap()
            .build();
        let (sender, receiver) = mpsc::channel(8);
        Self {
            config,
            registry,
            metrics,
            swarm,
            state: test_state(),
            sender,
            receiver,
        }
    }

    async fn handle(&mut self, event: SwarmEvent<NetworkEvent>) {
        let result = timeout(
            Duration::from_secs(2),
            handle_swarm_event(
                event,
                &self.config,
                &self.metrics,
                &mut self.swarm,
                &mut self.state,
                &self.sender,
            ),
        )
        .await
        .expect("event handler must not block");
        assert!(matches!(result, ControlFlow::Continue(())));
        while self.receiver.try_recv().is_ok() {}
    }

    async fn connect(&mut self, key: &Keypair, id: usize, count: u32) {
        let connection_id = ConnectionId::new_unchecked(id);
        self.handle(SwarmEvent::ConnectionEstablished {
            peer_id: key.public().to_peer_id(),
            connection_id,
            endpoint: endpoint(),
            num_established: count.try_into().unwrap(),
            concurrent_dial_errors: None,
            established_in: Duration::from_millis(1),
        })
        .await;
        self.handle(SwarmEvent::Behaviour(NetworkEvent::Identify(Box::new(
            identify::Event::Received {
                connection_id,
                peer_id: key.public().to_peer_id(),
                info: identify::Info {
                    public_key: key.public(),
                    protocol_version: self.config.protocol_names.consensus.clone(),
                    agent_version: "metrics-test".into(),
                    listen_addrs: vec!["/ip4/127.0.0.1/tcp/12345".parse().unwrap()],
                    protocols: vec![StreamProtocol::new("/ipfs/ping/1.0.0")],
                    observed_addr: "/ip4/127.0.0.1/tcp/12346".parse().unwrap(),
                    signed_peer_record: None,
                },
            },
        ))))
        .await;
    }

    async fn disconnect(&mut self, key: &Keypair, id: usize, remaining: u32) {
        self.handle(SwarmEvent::ConnectionClosed {
            peer_id: key.public().to_peer_id(),
            connection_id: ConnectionId::new_unchecked(id),
            endpoint: endpoint(),
            num_established: remaining,
            cause: None,
        })
        .await;
    }

    fn sum(&self, name: &str) -> f64 {
        let mut output = String::new();
        encode(&mut output, &self.registry).unwrap();
        output
            .lines()
            .filter(|line| {
                line.strip_prefix(name)
                    .is_some_and(|suffix| suffix.starts_with(['{', ' ']))
            })
            .map(|line| line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap())
            .sum()
    }

    fn assert_peers(&self, expected: f64) {
        for name in [
            "libp2p_identify_remote_protocols",
            "libp2p_identify_remote_listen_addresses",
            "libp2p_identify_local_observed_addresses",
        ] {
            assert_eq!(self.sum(name), expected, "{name}");
        }
    }
}

fn endpoint() -> ConnectedPoint {
    ConnectedPoint::Listener {
        local_addr: "/ip4/127.0.0.1/tcp/12346".parse().unwrap(),
        send_back_addr: "/ip4/127.0.0.1/tcp/12345".parse().unwrap(),
    }
}

#[tokio::test]
async fn metrics_remove_identified_peer_only_after_last_connection_closes() {
    let mut fixture = Fixture::new();
    let key = Keypair::generate_ed25519();
    fixture.assert_peers(0.0);
    fixture.connect(&key, 1, 1).await;
    fixture.assert_peers(1.0);
    fixture.connect(&key, 2, 2).await;
    fixture.assert_peers(1.0);
    fixture.disconnect(&key, 1, 1).await;
    fixture.assert_peers(1.0);
    fixture.disconnect(&key, 2, 0).await;
    fixture.assert_peers(0.0);
    fixture.connect(&key, 3, 1).await;
    fixture.assert_peers(1.0);
    fixture.disconnect(&key, 3, 0).await;
    fixture.assert_peers(0.0);

    assert_eq!(
        fixture.sum("libp2p_swarm_connections_established_total"),
        3.0
    );
    assert_eq!(
        fixture.sum("libp2p_swarm_connections_duration_seconds_count"),
        3.0
    );
    assert_eq!(fixture.sum("libp2p_identify_received_total"), 3.0);
}

#[tokio::test]
async fn metrics_do_not_accumulate_disconnected_peers_under_churn() {
    let mut fixture = Fixture::new();
    for id in 1..=128 {
        let key = Keypair::generate_ed25519();
        fixture.connect(&key, id, 1).await;
        fixture.assert_peers(1.0);
        fixture.disconnect(&key, id, 0).await;
        fixture.assert_peers(0.0);
    }
    assert_eq!(
        fixture.sum("libp2p_swarm_connections_established_total"),
        128.0
    );
    assert_eq!(
        fixture.sum("libp2p_swarm_connections_duration_seconds_count"),
        128.0
    );
}

#[tokio::test]
async fn metrics_record_fallback_swarm_events_once() {
    let mut fixture = Fixture::new();
    fixture
        .handle(SwarmEvent::Dialing {
            peer_id: Some(PeerId::random()),
            connection_id: ConnectionId::new_unchecked(1),
        })
        .await;
    assert_eq!(fixture.sum("libp2p_swarm_dial_attempt_total"), 1.0);
}

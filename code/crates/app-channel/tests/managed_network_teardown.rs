use std::time::Duration;

use arc_malachitebft_app_channel::app::config::{ConsensusConfig, ValueSyncConfig};
use arc_malachitebft_app_channel::app::metrics::SharedRegistry;
use arc_malachitebft_app_channel::app::spawn::spawn_network_actor;
use arc_malachitebft_app_channel::app::types::Keypair;
use malachitebft_engine::network::{
    Multiaddr, NetworkEvent, NetworkIdentity, NetworkMsg, NetworkRef,
};
use malachitebft_test::codec::json::JsonCodec;
use malachitebft_test::TestContext;
use ractor::{async_trait, call, Actor, ActorProcessingErr, ActorRef};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

struct Observer;

#[async_trait]
impl Actor for Observer {
    type Msg = NetworkEvent<TestContext>;
    type State = Option<oneshot::Sender<()>>;
    type Arguments = oneshot::Sender<()>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        connected: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(Some(connected))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if matches!(message, NetworkEvent::PeerConnected(_)) {
            if let Some(connected) = state.take() {
                let _ = connected.send(());
            }
        }
        Ok(())
    }
}

async fn spawn_network(key: u8, peers: Vec<Multiaddr>) -> NetworkRef<TestContext> {
    let mut config = ConsensusConfig::default();
    config.p2p.listen_addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    config.p2p.persistent_peers = peers;
    spawn_network_actor(
        &config,
        &ValueSyncConfig::default(),
        NetworkIdentity::new(
            format!("stop-order-{key}"),
            Keypair::ed25519_from_bytes([key; 32]).unwrap(),
            None,
        ),
        &SharedRegistry::new(Default::default(), None),
        JsonCodec,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn inbound_network_events_survive_stopped_consensus_subscriber() {
    let network = spawn_network(41, Vec::new()).await;
    let address = timeout(Duration::from_secs(10), async {
        loop {
            let dump = call!(network, NetworkMsg::DumpState).unwrap().unwrap();
            let address = dump.local_node.listen_addr;
            if address.to_string().starts_with("/ip4/127.0.0.1/tcp/")
                && !address.to_string().ends_with("/tcp/0")
            {
                break address;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let (closed, _) = oneshot::channel();
    let (subscriber, joined) = Actor::spawn(None, Observer, closed).await.unwrap();
    network
        .cast(NetworkMsg::Subscribe(Box::new(subscriber.clone())))
        .unwrap();
    call!(network, NetworkMsg::DumpState).unwrap();
    subscriber.stop(None);
    joined.await.unwrap();
    // Covers direct replay to a stopped subscriber as well as the existing
    // output-port subscription receiving traffic after consensus has stopped.
    network
        .cast(NetworkMsg::Subscribe(Box::new(subscriber)))
        .unwrap();
    let (connected, observed) = oneshot::channel();
    let (observer, observer_join) = Actor::spawn(None, Observer, connected).await.unwrap();
    network
        .cast(NetworkMsg::Subscribe(Box::new(observer.clone())))
        .unwrap();
    call!(network, NetworkMsg::DumpState).unwrap();

    let peer = spawn_network(42, vec![address]).await;
    timeout(Duration::from_secs(10), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(call!(network, NetworkMsg::DumpState).unwrap().is_some());

    observer.stop(None);
    observer_join.await.unwrap();
    peer.stop_and_wait(None, Some(Duration::from_secs(10)))
        .await
        .unwrap();
    network
        .stop_and_wait(None, Some(Duration::from_secs(10)))
        .await
        .unwrap();
}

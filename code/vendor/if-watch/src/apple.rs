use crate::{IfEvent, IpNet, Ipv4Net, Ipv6Net};
use core_foundation::array::CFArray;
use core_foundation::runloop::{kCFRunLoopCommonModes, kCFRunLoopDefaultMode, CFRunLoop};
use core_foundation::string::CFString;
use fnv::FnvHashSet;
use futures::channel::mpsc;
use futures::stream::{FusedStream, Stream};
use if_addrs::IfAddr;
use std::collections::VecDeque;
use std::io::{Error, Result};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use system_configuration::dynamic_store::{
    SCDynamicStore, SCDynamicStoreBuilder, SCDynamicStoreCallBackContext,
};

#[cfg(feature = "tokio")]
pub mod tokio {
    //! An interface watcher.
    //! **On Apple Platforms there is no difference between `tokio` and `smol` features,**
    //! **this was done to maintain the api compatible with other platforms**.

    /// Watches for interface changes.
    pub type IfWatcher = super::IfWatcher;
}

#[cfg(feature = "smol")]
pub mod smol {
    //! An interface watcher.
    //! **On Apple platforms there is no difference between `tokio` and `smol` features,**
    //! **this was done to maintain the api compatible with other platforms**.

    /// Watches for interface changes.
    pub type IfWatcher = super::IfWatcher;
}

#[derive(Debug)]
pub struct IfWatcher {
    addrs: FnvHashSet<IpNet>,
    queue: VecDeque<IfEvent>,
    rx: mpsc::Receiver<()>,
    _worker: Worker,
}

impl IfWatcher {
    pub fn new() -> Result<Self> {
        let (tx, rx) = mpsc::channel(1);
        let worker = Worker::spawn(tx)?;
        let mut watcher = Self {
            addrs: Default::default(),
            queue: Default::default(),
            rx,
            _worker: worker,
        };
        watcher.resync()?;
        Ok(watcher)
    }

    fn resync(&mut self) -> Result<()> {
        let addrs = if_addrs::get_if_addrs()?;
        for old_addr in self.addrs.clone() {
            if addrs
                .iter()
                .find(|addr| addr.ip() == old_addr.addr())
                .is_none()
            {
                self.addrs.remove(&old_addr);
                self.queue.push_back(IfEvent::Down(old_addr));
            }
        }
        for new_addr in addrs {
            let ipnet = ifaddr_to_ipnet(new_addr.addr);
            if self.addrs.insert(ipnet) {
                self.queue.push_back(IfEvent::Up(ipnet));
            }
        }
        Ok(())
    }

    /// Iterate over current networks.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.addrs.iter()
    }

    /// Poll for an address change event.
    pub fn poll_if_event(&mut self, cx: &mut Context) -> Poll<Result<IfEvent>> {
        loop {
            if let Some(event) = self.queue.pop_front() {
                return Poll::Ready(Ok(event));
            }
            match Pin::new(&mut self.rx).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(Error::other("Background task stopped")))
                }
                Poll::Ready(_) => {}
            }
            if let Err(error) = self.resync() {
                return Poll::Ready(Err(error));
            }
        }
    }
}

impl Stream for IfWatcher {
    type Item = Result<IfEvent>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).poll_if_event(cx).map(Some)
    }
}

impl FusedStream for IfWatcher {
    fn is_terminated(&self) -> bool {
        false
    }
}

fn ifaddr_to_ipnet(addr: IfAddr) -> IpNet {
    match addr {
        IfAddr::V4(ip) => {
            let prefix_len = (!u32::from_be_bytes(ip.netmask.octets())).leading_zeros();
            IpNet::V4(
                Ipv4Net::new(ip.ip, prefix_len as u8).expect("if_addrs returned a valid prefix"),
            )
        }
        IfAddr::V6(ip) => {
            let prefix_len = (!u128::from_be_bytes(ip.netmask.octets())).leading_zeros();
            IpNet::V6(
                Ipv6Net::new(ip.ip, prefix_len as u8).expect("if_addrs returned a valid prefix"),
            )
        }
    }
}

fn callback(_store: SCDynamicStore, _changed_keys: CFArray<CFString>, info: &mut mpsc::Sender<()>) {
    match info.try_send(()) {
        Err(err) if err.is_disconnected() => CFRunLoop::get_current().stop(),
        _ => {}
    }
}

#[derive(Debug, Default)]
struct Shutdown {
    stopped: AtomicBool,
    run_loop: Mutex<Option<CFRunLoop>>,
}

#[derive(Debug)]
struct Worker {
    shutdown: Arc<Shutdown>,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn spawn(tx: mpsc::Sender<()>) -> Result<Self> {
        let shutdown = Arc::new(Shutdown::default());
        let worker_shutdown = Arc::clone(&shutdown);
        let thread = thread::Builder::new()
            .name("if-watch".into())
            .spawn(move || background_task(tx, worker_shutdown))?;
        Ok(Self {
            shutdown,
            thread: Some(thread),
        })
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown.stopped.store(true, Ordering::Release);
        if let Some(run_loop) = self.shutdown.run_loop.lock().unwrap().as_ref() {
            run_loop.stop();
        }
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                log::error!("Interface watcher thread panicked");
            }
        }
    }
}

fn background_task(tx: mpsc::Sender<()>, shutdown: Arc<Shutdown>) {
    let Some(store) = SCDynamicStoreBuilder::new("global-network-watcher")
        .callback_context(SCDynamicStoreCallBackContext {
            callout: callback,
            info: tx,
        })
        .build()
    else {
        log::error!("Failed to create SCDynamicStore");
        return;
    };
    store.set_notification_keys(
        &CFArray::<CFString>::from_CFTypes(&[]),
        &CFArray::from_CFTypes(&[CFString::new("State:/Network/Interface/.*/IPv.")]),
    );
    let Some(source) = store.create_run_loop_source() else {
        log::error!("Failed to create run loop source");
        return;
    };
    let run_loop = CFRunLoop::get_current();
    run_loop.add_source(&source, unsafe { kCFRunLoopCommonModes });
    *shutdown.run_loop.lock().unwrap() = Some(run_loop.clone());
    while !shutdown.stopped.load(Ordering::Acquire) {
        // Stop can race the entry into CFRunLoopRunInMode. The finite interval
        // bounds that race even if no interface event ever arrives.
        CFRunLoop::run_in_mode(
            unsafe { kCFRunLoopDefaultMode },
            Duration::from_millis(100),
            false,
        );
    }
    *shutdown.run_loop.lock().unwrap() = None;
}

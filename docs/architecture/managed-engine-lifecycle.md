# Managed engine lifecycle (F3 implementation plan)

Status: implementation in progress, with real-engine Linux lifecycle tests passing.
Platform CI and broader fork qualification remain incomplete. Kontor protocol
upgrade phase F3, fork base `2b3e1adf19d90e7289ed064501ca4dd74ca3027d`.
Interlink agreement: `e4778a0e596aeb63d8a99dfe5144404d`.

A managed engine owns a dedicated OS thread and Tokio runtime. Build all actors
and transport resources inside that runtime. The node remains alive and uses
in-process channels. Only joining the dedicated thread after runtime destruction
establishes completion. A timed-out wait retains the owner and cannot authorize
starting another engine. Do not use Tokio shutdown_timeout or shutdown_background,
which permit unfinished work to survive the return.

## Ownership and failure contract

- Inject a fresh engine metrics registry. Never register managed engine metrics in
  SharedRegistry::global(); the node selects the active registry for export.
- Reject externally constructed custom actors for managed startup unless their
  complete ownership and join contract can be transferred. An ActorRef alone does
  not establish ownership of its runtime or spawned tasks.
- Request graceful actor shutdown before dropping the runtime. Account for ractor
  stopping a supervisor killing its children, and post_stop not running on actor
  failure or kill. WAL release must therefore also work without post_stop.
- Explicitly join the WAL OS thread after closing its command sender. Complete
  flush/fsync before reporting successful orderly stop. Propagate worker or flush
  failures rather than reporting clean shutdown.
- Drop the runtime on its dedicated thread, outside any entered async context.
  Runtime destruction owns nested Tokio tasks, including libp2p/quinn tasks.
- Preserve a single join owner across timeout and cancellation. A panic or
  unexpected actor exit is an engine fault, never permission for silent restart.
- Partial startup failure and canceled readiness use the same teardown path.
  Track the supervisor and children independently of the final EngineHandle, so
  builder errors and panics cannot skip cleanup. Catch builder panics inside the
  owned runtime, while actor cleanup can still run.
- Do not drop a pending actor-spawn future to cancel startup: ractor awaits
  pre_start before returning a handle. Stop invalidates the readiness receiver
  immediately, but waits for startup to return before draining actors. A wedged
  startup retains the owner and makes wait_stopped time out.
- Before stopping children, acknowledge PrepareShutdown on their supervisor.
  Stop children, allow one second of grace, kill remaining actors, and wait for
  terminal status. A second ordinary supervisor RPC drains terminal supervision
  events before stopping the supervisor. Record the first safety or child failure
  even during teardown, then report it after runtime and WAL destruction.
- Do not use repeated ractor ActorCell::wait calls across the grace timeout:
  ractor 0.15.10 retains only one notification permit for late waiters. Poll
  terminal status instead. Runtime destruction and the outer thread join remain
  the final completion boundary, including nested transport tasks.
- Keep lifecycle control runtime-agnostic, with standard types for stop requests,
  terminal outcomes and waiting. Later bundle adapters must not require the
  caller to enter a different copy of Tokio.
- Closing the engine side must release pending response senders/receivers.
  External application replies retained across shutdown cannot revive the engine.
  Node-side generation fencing and durable signing remain K5 responsibilities.

## Confirmed dependency issue

The fork lock uses if-watch 3.2.1. Its Apple IfWatcher::new detaches a standard
thread running CFRunLoop. Dropping the watcher only disconnects its channel; the
run loop stops only when a subsequent network event invokes its callback. A quiet
network therefore leaves the thread alive. Published 3.2.2 and the inspected
upstream master still have this lifecycle behavior.

The explicit F3 amendment is agreed in Interlink receipt
`3e638a609befda34acc1367f4663c0e3`: patch if-watch 3.2.2, the version already
shipped by Kontor, and update the fork lock to qualify that same version.
Published archive SHA256:
`71c02a5161c313f0cbdbadc511611893584a10a7b6153cb554bdf83ddce99ec2`.
The version transition also changes apple.rs, linux.rs and win.rs; do not treat
this as a source-identical version change. Qualify those existing upstream deltas
with the actual fork and node integration tests.

Own the watcher thread and retain its run-loop reference. Drop sets a stop flag,
calls CFRunLoopStop for prompt wakeup, and joins the thread. Finite run-loop
intervals of at most 100ms provide a backstop for shutdown arriving before the
run loop starts. Preserve interface event handling and test worst-case drop
latency without inducing a network-change event. Initial resync failure must
also drop/join. Version 3.2.2 already handles a closed background channel with an
error, so no separate closed-channel backport is needed.

CI must reconstruct the vendor from the checksum-verified pristine archive plus
recorded patch. K1 must fail if any consensus bundle resolves if-watch without
the patched path source; Cargo patches from a dependency manifest do not propagate
to the bundle's workspace. Linux's netlink task/socket and Apple's run-loop thread
must both return to the isolated test-process resource baseline.

Sources checked:

- https://docs.rs/crate/if-watch/3.2.1/source/src/apple.rs
- https://raw.githubusercontent.com/libp2p/if-watch/master/src/apple.rs
- https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#shutdown
- https://docs.rs/ractor/0.15.10/src/ractor/actor.rs.html (lines 828-843 confirm
  kill/error skips post_stop in the fork's locked release).

## Acceptance evidence to implement

Use actual Malachite engines, not only fake task handles. Repeatedly start and
stop about 20 times on the same TCP/QUIC port and WAL path. Retain old application
channels and pending replies while stopping. Exercise startup failure after
network creation, pending connector replies, worker failure, blocked WAL work,
blocked Tokio work and a cancelled/timed-out wait followed by a successful join.
A timeout must never report released ownership.

On Linux and macOS, establish an isolated test-process baseline after deliberate
runtime warmup. After each completed stop, assert threads, open file descriptors
and network bindings return to baseline. Check that metric family/series counts
do not grow and scrapes remain valid. Exercise unspecified listen addresses so
interface watchers are created. Preserve resolved listen-address behavior.

Test engine panic containment with the outer node/runtime still responsive, and
ensure failure remains observable through the join result. Verify naming does
not introduce process-global ractor registry entries. Qualify both orderly and
partial-start cleanup on the real platform CI runners.


## Local implementation evidence

The integration test uses actual Malachite validators, transport stacks, actors,
WAL workers and per-engine metrics. On Linux it currently passes:

- Twenty restart cycles per TCP and QUIC transport on the same port and WAL path,
  after warmup. Every completed stop returns thread/file counts to baseline,
  permits rebinding the port, closes held application channels and pending replies,
  and preserves a stable set of metric families.
- Partial startup failure after network creation, using an invalid WAL path.
- Real WAL replay blocked inside its codec. Repeated shutdown waits, including a
  two-second wait beyond the one-second actor grace period, retain the join owner.
  The engine network channels close, the outer runtime heartbeat continues, and
  releasing the worker permits a complete join.
- A real WAL codec panic and a real consensus actor panic, including a panic
  released after shutdown has begun. Both remain observable
  as faults after complete teardown; simultaneous engine and WAL errors are kept.
- An engine-owned Tokio blocking task outliving the actor grace period. A stop
  cannot complete while that task is blocked, and the outer runtime stays alive.
- Twenty direct interface-watcher drops, both immediate and after settling. Linux
  netlink descriptors return to baseline without requiring interface changes.
  The same test checks the Apple watcher thread and drop latency on macOS, where
  it has not yet run.

- Cancellation during partial startup while real actors, WAL and network already
  exist. A blocked builder keeps the owner alive, prevents channel delivery, and
  does not block the outer runtime. Released startup drains all tracked actors
  and restores the resource baseline.
- A real WAL worker's final flush failure, injected through its I/O callback.
  Joining preserves the error and the file's advisory lock is released for reopen.

- Panic during partial startup after actor creation. The panic remains observable
  after actor cleanup, runtime destruction and WAL joins, with resources at baseline.

The latest complete lifecycle run passed in 54.34s, with worst Linux watcher drop
latency 10.542 microseconds. All 54 engine unit tests pass, including the new first
safety-fault preservation and final WAL flush-failure cases. These are local
observations, not cross-platform guarantees.
The all-feature workspace unit selection passed 759 tests (three existing skips)
at `b92e3909a3289ad10051eef0e00adf488ddcd729`, before the WAL correction below.

Independent review identified an error-loss race: a nonpanic WAL operation error
could arrive after shutdown killed the waiting actor, then a successful final
flush could hide the failure. The worker now retains its first operation error,
including replay's nested entry failures, independently of reply delivery. It
still attempts final flush and preserves both errors when that also fails.
The real-engine regression delays a replay error until after the actor grace
period and confirms the join returns a fault with resources at baseline. A real
worker test also verifies append encoding failure plus final-fsync failure with
an already-closed reply receiver. Best-effort diagnostic Dump remains unchanged.
The vendor reconstruction and formatting checks pass under Rust 1.99.1.
Full-workspace all-target, all-feature Clippy passes on the WAL correction. Existing async-trait and
async-recursion macros required narrowly scoped redundant-must-use lint
annotations for that compiler; their generated behavior is unchanged.

Outstanding before F3 review/PR: qualify macOS watcher ownership, run the broader
fork checks, and adapt inherited upstream-only CI runner/policy configuration for
this fork. The lifecycle implementation is being recorded for review; no F3 PR
has been created.

Kontor PR https://github.com/KontorProtocol/Kontor/pull/605 corrected the post-F2
Cargo Chef regression and merged as `6504523fa674211d8781c26905961da090f94efb`.
Both reviews and exact-head PR checks passed. Post-merge main CI40489996759,
native CI40489997143 and mobile CI40489996758 passed. Main CI includes both cache
builds, both architecture images, and successful multiarch publication. The
post-merge prerequisite for F3 commits is satisfied. No advisory exceptions added.

## F2 prerequisite evidence recorded at F3 start

Kontor PR https://github.com/KontorProtocol/Kontor/pull/604 merged as
`28789f61e1622a5c631ef7acecbab9ffbd532577` at 2026-10-27T09:17:04Z.
The merged tree equals tested/reviewed head
`26be2a72884fd7536b331d0efaa6eb3c2f5b49bd`. CI40476490470,
native40476490171, mobile40476490159 and diagnostics40476490036 passed.
Both reviews are complete and no advisory exception was added.

The owner-private dark Site was updated from source
`2cb234dff06e1ca942f7bda48978b53ff4630bab`:
https://kontor-protocol-study-20261026.wilfred714608.chatgpt.site#implementation
Deployment `appgdep_6ae06c97d8208191b5a33441d7341274` succeeded. Static
structure, anchors, dark theme and downloads passed; this text-only update did
not receive new browser visual verification.

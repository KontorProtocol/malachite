# Managed engine lifecycle (F3 implementation plan)

Status: source reviews complete and all three platform runs passed before rebase.
Final PR qualification remains pending. Kontor protocol upgrade phase F3,
original fork base `2b3e1adf19d90e7289ed064501ca4dd74ca3027d`, now rebased onto
verified CI bootstrap merge `0821ca5ebbe29d538a2e7edbd49990010da8326d`.
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
  Any non-runtime resource created per engine, including a durable writer needing
  final flush, must register an explicit lifecycle owner, as the WAL does.
  Process-lifetime services have a separate explicit contract below; they cannot
  retain engine authority or per-engine resources after teardown. Thread/descriptor
  baselines guard this audited ownership rule.
  Dropping ManagedEngine in a Tokio runtime context logs an error before joining;
  the guard does not panic or abandon ownership.
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
  The same test checks the Apple watcher thread and drop latency on macOS.
  Initial macOS platform CI passed; final-head qualification remains required.

- Cancellation during partial startup while real actors, WAL and network already
  exist. A blocked builder keeps the owner alive, prevents channel delivery, and
  does not block the outer runtime. Released startup drains all tracked actors
  and restores the resource baseline.
- A real WAL worker's final flush failure, injected through its I/O callback.
  Joining preserves the error and the file's advisory lock is released for reopen.

- Panic during partial startup after actor creation. The panic remains observable
  after actor cleanup, runtime destruction and WAL joins, with resources at baseline.

The WAL-correction lifecycle run passed in 54.34s, with worst Linux watcher drop
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

Outstanding before F3 PR: resolve the Linux resource-observation failure below,
qualify the final head on all three platforms, run the broader fork checks, and
merge the reviewed fork CI prerequisite. The lifecycle implementation is being recorded for review; no F3 PR
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


## Review dispositions

Independent source review and WAL-correction follow-up approved head
`974a0b71cd3fd3f7686201fb0a741bdc39d457db`. Interlink source approval:
`6f7ad68e65a46c1279e5fce8ec9026a8`, also recorded under the dedicated source
review task in receipt `555b86af987e621c765928791759e2b9`.

- F3-1: checksum-and-patch vendor verification must run in F3 CI whenever vendor
  or verifier files change. Still a pre-PR gate.
- F3-2: retained ordinary operation errors must not add an extra SafetyFailure
  notification on unmanaged worker exit after a successful final flush. Only a
  terminal panic or final-flush failure produces that extra notification. Every
  operation failure still survives in the join outcome. A closed-reply worker
  test checks both notification behavior and combined final-flush failure.
- F3-3: document the nested-runtime-resource ownership rule above.
- F3-4: log runtime-context Drop misuse, preserve the blocking join.
- F3-5: real Linux x86/ARM and macOS lifecycle CI remains required.

Interlink accepted these dispositions in `e8910aced07336bf8d92f8fc7b1ea06c`.
The behavior and CI changes need exact-head follow-up reviews.


The F3 CI workflow is prepared for real lifecycle and engine-unit tests on
ubuntu-24.04, ubuntu-24.04-arm and macos-latest with Rust 1.99.1. It verifies
vendor provenance on every platform, and its path triggers include code/vendor
and tools/lifecycle-vendor changes. Feature-branch pushes allow qualification
before opening the reviewed PR. Initial platform results are recorded below.
The F3-2 notification regression and all 54 engine tests pass locally; affected
all-target/all-feature Clippy also passes after the Drop diagnostic.
Fork CI prerequisite PR: https://github.com/KontorProtocol/malachite/pull/2.
F3 must rebase on its verified merge before PR completion.


## Platform qualification and Linux resource observation

Run 40498425174 at `18e2e745dba69fd6fdf986d9706c866bd4f0e43d` passed
on Linux ARM and macOS, including all 54 engine unit tests per platform.
The macOS lifecycle fixture took 58.12s; its worst of twenty quiet watcher drops
was 581.417 microseconds. The drop bound is tightened from two seconds to 500ms,
which still covers the 100ms run-loop backstop with scheduling margin.
Linux x86 failed after an injected actor panic with six threads against a
five-thread baseline, while all twelve file descriptors were already at baseline.

Diagnostic head `5b2642e66b9c07227abc3b259ed6458a1113aa89` preserves those
immediate assertions and names the outer test runtime. The local fixture then
reproduced the same symptom after a WAL panic: the extra task was the joined
`malachite-engin` owner thread, flags 4194380 (`0x40004c`, including
`PF_EXITING`), with zero vsize and all address-space fields cleared. The test
runtime workers and other baseline tasks were unchanged. A separate 20,000-run
standard-thread spawn/join probe saw this visibility delay 576 times, with a
maximum 202.377 microseconds.

Linux clears the join futex during address-space release before removing the
thread's proc entry. The measurement omits entries already gone during stat reads and waits at most
one second for identified entries with both PF_EXITING and zero vsize to disappear.
It freezes the initially observed live-thread count while waiting, so a second
live worker cannot disappear into an unrelated exit wait. It applies the same
rule to baselines. Live extra threads are never waited
out; descriptors are counted before the wait and still compared immediately.
The parser splits after the last ')' so thread names containing spaces or
parentheses cannot shift the fields. A real blocked extra-thread control proves
that the measurement still detects a live worker. A mixed control releases and
joins that real worker during a simulated exiting-entry wait and requires its
initial live observation to remain counted. Waits, timeouts and mismatches
retain task-state diagnostics; successful waits report re-poll count and elapsed
time. This changes test observation only, not engine shutdown or joining.

Primary kernel sources:
- https://github.com/torvalds/linux/blob/v6.8/kernel/exit.c
- https://github.com/torvalds/linux/blob/v6.8/fs/proc/array.c

The narrower measurement rule was agreed with the reviewer in receipt
`81fe17a4ee6cbed5c263e574f31ff067`. Final local and platform results, including
re-poll counts, are pending. The first platform run is not final merge evidence.

Independent follow-up found that re-enumerating all tasks during a wait could
hide another worker that was initially live. That first proposed observer was
never committed or pushed. Its Interlink approval `77c48f41e9f2f3b05f2b373471467cc7`
is withdrawn and superseded by `15533d850d739eba00bd1068d1ffb9f0` for the fixed
initial-observation implementation. Independent follow-up also approved the fix.
The final local fixture passed in 54.20s, including the mixed live/exiting control;
all-target/all-feature app-channel Clippy passed. Worst local watcher drop was
9.209 microseconds. The mixed control required one re-poll in 21.75 microseconds;
this is injected-control evidence, not a measured kernel-exit delay. Final-head
platform CI must record real exiting-task waits separately.

Diagnostic run 40500496729 at `5b2642e6` passed Linux x86 and macOS. Linux ARM
captured the same exit window at QUIC cycle two: extra owner task flags
`0x40004c`, zero vsize, twelve file descriptors already at baseline. This further
supports the measurement correction; the diagnostic run itself remains failed.


## Explicit process-lifetime services

The locked futures-timer 3.0.3 fallback creates one helper lazily per independently
linked crate graph. `TimerHandle::default` deliberately forgets its helper's join
handle after winning the fallback registration. That helper parks between events
and owns a timer heap; it holds no sockets or file descriptors and does not poll
engine futures. It only wakes task wakers. Destroying the runtime drops the
engine's Delay futures: Delay::drop clears each deadline, queues its cancellation
and wakes the helper, whose poll removes the heap entry. Physical reclamation of
canceled notification records is asynchronous; this is not a claim that the timer
heap is synchronously empty at engine join. A stale Tokio waker cannot execute an
already destroyed engine future or restore its signing/storage authority.

This helper is bundle-resident, not recreated or joined at each engine handoff.
K1 records its ownership per consensus graph, forbids cross-graph fallback sharing,
and keeps each graph resident for the node process lifetime. Wasmtime's separately
recorded per-graph Rayon pool and resident signal handlers follow their F1 ownership
contracts. These are named exceptions, not permission to detach new engine workers.
Any new service exception needs an explicit review showing bounded growth per
graph, no retained per-engine state or authority after teardown, and no leaked
engine descriptors. The repeated-cycle resource test remains mandatory.

K5/K7 node resource tests must warm each relevant graph with an engine start/stop
before taking their stable baseline. They must not tolerate an unexplained extra
thread per engine. Current evidence shows the same futures-timer helper across
repeated cycles. Documentation clarification agreed in Interlink receipt
`33e55cc703d83f1c0e077ae28de25ac8`; no production behavior changes.

Source: futures-timer 3.0.3 native/global.rs, native/timer.rs and native/delay.rs
from the checksum-locked registry package in code/Cargo.lock.


## Rebased source and complete pre-PR platform results

All three jobs in run40501837563 passed at
`cdc0ea41776bdf2135f0267ccbf2e3ea05e39785`, each with one complete real lifecycle
fixture and all54 engine unit tests:

| Platform | Lifecycle duration | Worst quiet watcher drop |
| --- | --- | --- |
| Linux x86 |54.12s|17.302 microseconds|
| Linux ARM |54.13s|52.89 microseconds|
| macOS |58.06s|702 microseconds|

The x86 run observed an actual exiting engine owner, requiring two re-polls in
42.008 microseconds. Its mixed control required one re-poll in59.05 microseconds.
ARM observed only the injected mixed control, one re-poll in36.825 microseconds.
These distinct cases are recorded separately; the real exit case satisfies the
observer's platform-evidence gate (Interlink receipt99f1e7be669a96d47dbbfac8066079d1).

The resident-timer prose was approved independently and by Interlink in
558607a501a8b37c048f08e29465b558. Local doc head30a0d696 was then rebased onto
bootstrap merge0821ca5e. All six commits were signed with the existing registered
key. Rebased head6078090f has byte-identical code, tests, lifecycle vendor/tooling,
platform workflow and lifecycle documentation to30a0d696. The only additions from
rebase are the reviewed bootstrap CI files and its ledger. This following ledger
update records the merge and restored policies. F3 PR CI must still run on its
actual final head, including the restored collaborator/title/signature gates.

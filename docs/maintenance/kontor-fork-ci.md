# Kontor fork CI prerequisite

Status: implementation, local validation and both source reviews complete.
The temporary policy bootstrap is recorded below; PR CI evidence is pending. This is a prerequisite PR before managed-engine phase F3.
Base: `2b3e1adf19d90e7289ed064501ca4dd74ca3027d`.
Plan agreement: Interlink receipt `01c5ded19810e657570fde30732c5b58`, accepted in
`4603fdb96bf63129ec8f2a74e3cce822` (B1 through B5).

## Resulting behavior

The inherited Circlefin runner labels have no matching runners in the Kontor
fork (GitHub runner API returned zero). Jobs now use repository variables
`CI_RUNNER_SMALL` and `CI_RUNNER_LARGE`, falling back to `ubuntu-24.04`.
Set either variable to another appropriate runner label when necessary. The
workflow validator is explicitly pinned to ubuntu-24.04 x86 to match its
checksum-pinned Linux x86 actionlint binary.

Rust qualification and Clippy use exactly Rust 1.99.1. The MSRV job still uses
cargo-msrv to verify the version declared by the crate, currently 1.88, independently
of its driver's compiler. Existing generated async-trait/async-recursion
`must_use` warnings get narrowly scoped allowances at the macro sites. No
production algorithm, dependency version, lockfile or public API changes.

Retained checks include workspace unit tests, discovery and application integration
through cargo-maelstrom, no_std, MSRV, standalone features, full Clippy, formatting,
Quint typechecking/tests, model-based tests, spelling, semver and coverage generation.
Workflow changes select the relevant checks, so the prerequisite must demonstrate
actual nonzero test counts. The obsolete removed Starknet MBT package selector is
removed; the existing Malachite MBT package still runs. Unit-test retries are
explicitly zero. Coverage no longer hides failing tests with ignore-run-fail.

Coverage generation and downloadable artifacts run on the fork. Only Codecov
uploads are guarded for upstream, whose secrets they require. Documentation still
builds and uploads its artifact; Pages publication remains upstream-only. Existing
crates.io publication, issue labeling, TODO issue generation and link-check issue
creation are kept behind upstream repository guards. The link check itself stays
available. None of these guards constitute passing publication/coverage-upload
evidence for the fork.

The fork PR gate checks that the PR author has write, maintain or admin access to
KontorProtocol/malachite. Bot authors without those collaborator permissions,
including Dependabot, fail this policy; the fork currently uses maintainer-authored
PRs. Upstream's contribution gate stays unchanged under its
own repository guard. Title validation and verified-commit checks are retained.
Sign with an existing registered local key when available. GitHub's
createCommitOnBranch API can also automatically sign commits, but this session's
OAuth token lacks workflow scope and GitHub rejected that mutation for the CI
changes. No code was committed by that attempt. The user's existing local GPG
key matches public registered signing key B597F1D5A69A5A5C; use it without changing
account trust or requesting additional token scopes. Verify the signature and
exact reviewed tree on the remote commit. Do not disable signature verification.

Official commit-signing API reference:
https://docs.github.com/en/graphql/reference/commits#createcommitonbranch

## Validation and bootstrap sequence

- Actionlint 1.7.12 passes locally. Its Linux ARM64 archive matched official
  SHA256 `325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6`.
  The CI Linux x86 archive is independently pinned to official SHA256
  `8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8`.
- All 25 retained third-party action revisions were verified through their
  official GitHub repository commit API. No floating action references added.
- Formatting, git diff whitespace checks and full-workspace all-target/all-feature
  Clippy pass on the prerequisite's original locked dependency graph.
- Both independent and Interlink exact-head reviews are required before the PR.
- Immediately before opening the reviewed PR, temporarily disable only `PR Gate`
  (workflow ID `309324264`) and `PR` (workflow ID `309324265`) through
  `gh workflow disable`. Record exact UTC timestamps here. These workflows use
  pull_request_target and cannot read their corrected definitions from the PR head.
  No test or security workflow is disabled.
- Merge only with current-head substantive CI green and actual test-count evidence.
  Record each run, counts, head, merge and post-main outcomes here.
- Immediately re-enable those two workflow IDs after merge. Record timestamps and
  verify their active states. The next PR, F3, must run and pass the new collaborator,
  title and signature policies before it can merge.
- Rebase the isolated F3 work on this merged prerequisite, retain both reviews,
  and qualify the real lifecycle on Linux x86, Linux ARM64 and macOS. F3 must also
  wire checksum-and-patch vendor verification into CI for vendor/tool changes.

## Execution ledger

Both required source reviews approved exact head
`d3fd6118cb67d68b86bb882bdf7ab1f4b4117320` against the stated base.
Independent follow-up confirmed the Clippy correction and architecture pin with
no remaining material findings. Interlink final approval is receipt
`27a507d4fd1c4a81a2666622e5b23dcc`. GitHub verified the head signature as valid;
remote and local tree are `a5d1438d6effba585e6de1995d1bf5b08060e077`.
This ledger-only follow-up records source review and operational evidence.



Independent review found the inherited actions-rs/clippy wrapper ignores Cargo's
exit status. A negative control against the pinned bundled action used a stub
Cargo emitting a valid lint error and exiting 101; the action printed the error
but exited zero. The workflow now runs Cargo directly so compiler and lint
failures fail the job. The full local Clippy command already passed with this
scope. No test or compiler failure is converted to success by a wrapper.

Policy bootstrap state (UTC, verified disabled_manually):
- PR Gate, workflow ID `309324264`, disabled `2026-10-27T11:37:54.933867+00:00`.
- PR, workflow ID `309324265`, disabled `2026-10-27T11:37:56.050199+00:00`.

No other workflow was disabled. Re-enable these exact IDs immediately after the
bootstrap merge and verify their active state. PR/current-head CI and post-main
results are still pending. No merge has occurred.

Do not reintroduce wrapper actions for compiler or lint steps unless their exit
status propagates compiler/lint failures to the job.

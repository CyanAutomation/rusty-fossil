# Phase 2C-A.3 integration map

**Updated:** 2026-11-27
**Status:** Integration gaps remain; no canary has run

This map traces the requested authorized execution path against the current branch. It is a code review aid, not evidence that a component has been exercised end to end.

| Path stage | Existing component | Missing connection or proof |
| --- | --- | --- |
| Soyuz authorization and durable run identity | `AuthorizationCoordinator`, `SoyuzApi`, and SQLite descriptor/claim/start records | `crates/rusty-fossil/src/main.rs` does not construct the coordinator, recovery manager, callback replay loop, Queue intake, or supervisor IPC client. |
| Repository and immutable workspace | `fossil-workspace` restricts Git transport and commit-pinned checkout; schema v7 stores repository/ref/commit/workspace identity | No controller lifecycle persists preparation intent/result or calls the library; no prepared workspace has been created. |
| Durable task prompt | Pi wrapper reads `/run/fossil/task-prompt.txt` with bounded read-only validation | No trusted path atomically stages, hashes, permissions, mounts, recovers, or cleans a per-run payload. |
| Trusted Pi execution | Supervisor has durable Docker launch/recovery for `Synthetic` | `DockerRuntimeConfig` and launch validation reject `PiCodingV1`; no separate Pi image/resource profile is admitted. |
| Restricted inference | Broker validates a fixed model and run-fenced signed token; ledger records request/log IDs and estimated Cloudflare cost | No supervisor capability issuer, active-run revocation service, worker attachment, or real Pi-to-broker test exists. No per-run spending, request-count/rate, or inference-token budget is applied. |
| Worker network boundary | Dormant Compose overlay declares an internal worker network and broker egress network | Host-enforced worker-only-to-broker egress, IPv4/IPv6/DNS/metadata/Engine denial, and restart persistence have not been demonstrated. |
| Pi result and callback | Pi JSONL parser, stream seal, outcome types, and durable callback outbox exist | Supervisor launch path cannot produce these Pi results. No real Pi structured stream, callback delivery, or trusted file diff verification has been observed. |
| Cancellation, restart, cleanup, retention | Synthetic supervisor/coordinator lifecycle has tests and durable records | Pi-specific preparation, broker revocation, prompt/workspace cleanup, cancellation races, recovery, and retention remain unimplemented or untested. |
| Agentlab canary | Read-only ARM64/Compose preflight completed; listener was confirmed expected by operator | No software deploy, repository fetch, provider inference, canary task, native lifecycle test, or resource measurement was performed. |

The broker change in this branch records request IDs and Cloudflare log IDs, and exposes observed estimated cost from the Cloudflare log API. It does not impose a per-run cost, request-count, rate, or inference-token budget. The cost report is intended for Soyuz's `progress.updated` event path and cost-per-PR accounting. The coordinator now supports durable, idempotent event enqueue; no supervisor runtime fetches the report or calls it yet. The ledger has no 100,000-run admission ceiling. A data-handling route approved for repository content and task prompts remains to be established.

## Live canary disposition

The Docker Engine TCP listener observed on Agentlab is expected per operator confirmation and is not treated as a blocker here. Its reachability and authentication were not probed. The read-only Cloudflare check found request logging enabled and zero data retention disabled. No inference request or configuration change was made. Rusty-Fossil will report observed cost; Soyuz will own cost-per-PR accounting and the operator will manage cost per run there.

The primary blockers are the software integration gaps listed above and the absence of an approved data-handling route for a canary. Production Queue intake remains disabled. See [Agentlab validation](AGENTLAB-VALIDATION.md) and [Phase 2C-A operations](PHASE-2C-A-OPERATIONS.md) for the point-in-time host and operational record.

# ADR 0014: Keep Pi model credentials behind a run-scoped broker

**Status:** Proposed; pilot profile remains disabled

## Context

Pi is a shell-capable coding worker. A provider credential placed in its environment or filesystem is available to Pi tools, and Docker bridge networking does not restrict the worker to one destination. Cloudflare AI Gateway logging and retention remain operator-managed account policy. Zero data retention is not required or desired for this project, and Rusty-Fossil will not enable it. Per-run monetary spending ceilings are out of scope.

## Decision

The intended design keeps the reusable Cloudflare AI Gateway credential in a separate Rust broker. The worker receives only a short-lived signed bearer capability bound to the canonical run ID, worker ID, claim callback ID, and start event ID. The broker accepts a bounded OpenAI-compatible request for one configured model and forwards it to one configured Cloudflare AI Gateway compatibility URL. The prototype refuses redirects, validates TLS, caps request and response bodies, output tokens, request count, per-run bytes, request rate, and upstream time, and persists only run fences and counters in a private SQLite ledger.

Pi workers should have an internal-only Docker network connection to the broker and no direct egress network. The broker may join the supervisor's egress network for its fixed upstream. This Compose layout is scaffolding only: it does not establish that a worker cannot reach host services, and host firewall or equivalent enforcement has not been verified on a deployment target. A controlled, local-only trusted LAN is the host deployment assumption; it does not extend to Pi containers. An operator-managed Cloudflare Tunnel connector exposes only explicitly configured host services. It is not a worker egress control or a generic proxy for server-initiated requests; those use the host's default route unless separately routed through Cloudflare Gateway/WARP.

## Consequences and remaining gates

- Pi can read its run-scoped token from the mounted file and its generated Pi configuration; the token is not secret from Pi's shell tools. Its signature and expiry limit the capability, but the prototype has no cancellation/completion revocation path.
- The supervisor does not yet mint tokens, stage them before Docker creation, or attach Pi containers to the internal network.
- The project does not implement a per-run monetary spend ceiling. Request, rate, byte, output-token and time limits are technical usage bounds, not financial guarantees. Cloudflare's configured logging and retention policy is not changed by this project; zero data retention is not enabled.
- The internal network and broker fixed endpoint need end-to-end destination tests on the target ARM64 host, including host and metadata addresses, DNS/IPv6 behavior, and provider redirects.
- No production Queue, GitHub publication, or host-level configuration change is authorized by this ADR.

Until these gates are completed and the supervisor integration is reviewed, Pi workload records continue to be rejected before Docker create.

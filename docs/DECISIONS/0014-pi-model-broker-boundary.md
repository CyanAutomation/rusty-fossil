# ADR 0014: Keep Pi model credentials behind a run-scoped broker

**Status:** Proposed; pilot profile remains disabled

## Context

Pi is a shell-capable coding worker. A provider credential placed in its environment or filesystem is available to Pi tools, and Docker bridge networking does not restrict the worker to one destination. Any existing default gateway must be reviewed before use; a non-production configuration with explicit logging and spend controls has not been approved or tested.

## Decision

The intended design keeps the reusable Cloudflare AI Gateway credential in a separate Rust broker. The worker receives only a short-lived signed bearer capability bound to the canonical run ID, worker ID, claim callback ID, and start event ID. The broker accepts a bounded OpenAI-compatible request for one configured model and forwards it to one configured Cloudflare AI Gateway compatibility URL. The prototype refuses redirects, validates TLS, caps request and response bodies, output tokens, request count, per-run bytes, request rate, and upstream time, and persists only run fences and counters in a private SQLite ledger.

Pi workers should have an internal-only Docker network connection to the broker and no direct egress network. The broker may join the supervisor's egress network for its fixed upstream. This Compose layout is scaffolding only: it does not establish that a worker cannot reach host services, and host firewall or equivalent enforcement has not been verified on a deployment target.

## Consequences and remaining gates

- Pi can read its run-scoped token from the mounted file and its generated Pi configuration; the token is not secret from Pi's shell tools. Its signature and expiry limit the capability, but the prototype has no cancellation/completion revocation path.
- The supervisor does not yet mint tokens, stage them before Docker creation, or attach Pi containers to the internal network.
- The broker prototype has no dollar-spend ceiling. Configure an approved non-production gateway with suitable account-side budget controls before any live inference.
- The internal network and broker fixed endpoint need end-to-end destination tests on the target ARM64 host, including host and metadata addresses, DNS/IPv6 behavior, and provider redirects.
- No production Queue, GitHub publication, or host-level configuration change is authorized by this ADR.

Until these gates are completed and the supervisor integration is reviewed, Pi workload records continue to be rejected before Docker create.

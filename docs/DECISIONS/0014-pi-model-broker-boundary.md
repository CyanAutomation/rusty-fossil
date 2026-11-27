# ADR 0014: Keep Pi model credentials behind a run-scoped broker

**Status:** Broker prototype updated; Pi pilot profile remains disabled

## Context

Pi is a shell-capable coding worker. A provider credential placed in its environment or filesystem is available to Pi tools, and Docker bridge networking does not restrict the worker to one destination. Cloudflare AI Gateway logging and retention remain operator-managed account policy. Zero data retention is not configured by Rusty-Fossil.

## Decision

The intended design keeps the reusable Cloudflare AI Gateway credential in a separate Rust broker. The worker receives only a short-lived signed bearer capability bound to the canonical run ID, worker ID, claim callback ID, and start event ID. The broker forwards to one configured Cloudflare AI Gateway compatibility URL and model. It refuses redirects, validates TLS, applies transport safety bounds, and records request/log correlation. The broker strips `max_tokens` and `max_completion_tokens` so Pi's default output-token ceiling does not restrict provider requests; the provider/model still applies its own technical limits.

Rusty-Fossil does not impose a per-run monetary budget, inference-count limit, request-rate quota, or inference-token ceiling. The broker records Cloudflare's reported estimated cost and exposes it through an authenticated cost-report endpoint. The coordinator can durably enqueue this data to Soyuz as a `progress.updated` event with a stable per-run event ID. The supervisor still needs to fetch `/v1/cost` and call that method. Cost data is observational and does not gate inference. Cloudflare's estimate is not a guaranteed invoice total.

Pi workers should have an internal-only Docker network connection to the broker and no direct egress network. The broker may join the supervisor's egress network for its fixed upstream. This Compose layout is scaffolding only: it does not establish that a worker cannot reach host services, and host firewall or equivalent enforcement has not been verified on a deployment target. A controlled, local-only trusted LAN is the host deployment assumption; it does not extend to Pi containers. An operator-managed Cloudflare Tunnel connector exposes only explicitly configured host services. It is not a worker egress control or a generic proxy for server-initiated requests; those use the host's default route unless separately routed through Cloudflare Gateway/WARP.

## Consequences and remaining gates

- Pi can read its run-scoped token from the mounted file and its generated Pi configuration; the token is not secret from Pi's shell tools. Its signature and expiry limit authorization lifetime, not inference tokens or model usage. The prototype has no cancellation/completion revocation path.
- The supervisor does not yet mint tokens, stage them before Docker creation, or attach Pi containers to the internal network.
- The coordinator now validates and durably enqueues a complete cost report as a `progress.updated` event with a stable per-run event ID. The supervisor does not yet fetch `/v1/cost` or invoke the method; that runtime connection remains open.
- The internal network and broker fixed endpoint need end-to-end destination tests on the target ARM64 host, including host and metadata addresses, DNS/IPv6 behavior, and provider redirects.
- No production Queue, GitHub publication, or host-level configuration change is authorized by this ADR.

Until these gates are completed and the supervisor integration is reviewed, Pi workload records continue to be rejected before Docker create.

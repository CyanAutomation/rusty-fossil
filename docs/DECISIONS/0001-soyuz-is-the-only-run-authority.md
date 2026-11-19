# ADR 0001: Soyuz is the only run authority

- Status: accepted
- Date: 2026-11-18

## Context

Rusty-Fossil will eventually supervise untrusted repository tasks. A local submit route, retry queue, or fallback scheduler could create work Soyuz does not know about, weaken cancellation and idempotency, or start a second execution after an uncertain network result.

## Decision

Every Rusty-Fossil execution must originate from a Soyuz-owned run. The controller validates Queue input, reads canonical state, persists claim intent, uses the worker API to claim, persists the claim identity, waits for `/started`, persists the returned authorization proof, and only then may a future worker launcher accept the run.

Soyuz is mandatory. Missing configuration or an unavailable control plane prevents execution readiness. There is no local task submission route, standalone CLI run command, feature flag for bypassing Soyuz, or automatic fallback.

## Rejected alternatives

- Optional Soyuz integration: makes the trust boundary depend on deployment flags.
- Local jobs when Soyuz is unavailable: creates work without canonical ownership and cannot safely deduplicate an uncertain acceptance.
- A private `/next-job` API: conflicts with the current Cloudflare Queue pull contract and duplicates scheduling ownership.

## Consequences

The Phase 1 service is diagnostics-only and reports not-ready. Queue models are tested with fixtures, but no production Queue is consumed. Future changes that can launch a worker must receive the unforgeable authorization value produced by the Soyuz client and a matching durable record.

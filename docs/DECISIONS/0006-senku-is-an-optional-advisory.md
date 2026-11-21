# ADR 0006: Keep Senku optional and advisory

- Status: accepted
- Date: 2026-11-21

## Context

Rusty-Fossil needs reusable access to Senku's Kaseki-owned decision profiles while keeping Soyuz as the sole run authority and keeping production inference disabled until a credential and rollout are reviewed.

## Decision

Encapsulate Senku REST calls in `fossil-senku`, with typed inputs for the five published Kaseki profiles, versioned envelopes, bounded/redacted evidence, strict consumer-side response checks, no automatic retry, and a mockable `DecisionService` trait. Task admission is the only coordinator seam in this phase, and the returned decision is never an authorization input.

Senku origin/token configuration is optional and independent from Soyuz credentials. Never reuse the Soyuz worker API token. The current Senku permission matrix authorizes these profiles for the Kaseki identity; a dedicated Rusty-Fossil identity requires a separately reviewed Senku change. The production service does not instantiate the client.

## Consequences

Unavailable or invalid advice is recorded as degraded and cannot permit a failed deterministic check, claim, start, retry, validation bypass, execution, or publication. Per-profile client counters are available in the crate, but the service diagnostics endpoint does not expose them until a runtime explicitly wires the client. No JEV or provider integration is implemented in Rusty-Fossil.

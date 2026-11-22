---
name: external-contract-compatibility
description: Use when changing Soyuz, Cloudflare Queue, Senku, or other external request, response, identity, or lifecycle contracts consumed or emitted by Rusty-Fossil.
---

# External contract and compatibility changes

Use this skill when a wire format or external behavior changes. For lifecycle changes, combine it with the [Soyuz run lifecycle skill](../soyuz-run-lifecycle/SKILL.md).

## Workflow

1. Read the current authoritative contract and the relevant sections of [CONTRACTS.md](../../../docs/CONTRACTS.md) and [COMPATIBILITY.md](../../../docs/COMPATIBILITY.md). Soyuz's current published contract takes precedence for Soyuz-facing behavior; Kaseki evidence is comparative, not authority.
2. Record the affected owner, endpoint or message, version, identity fields, validation rules, retry semantics, and applicable requirement IDs. Separate canonical IDs and fences from transport IDs such as Queue message and lease IDs.
3. Add or update recorded JSON fixtures and contract tests first. Cover valid and invalid payloads, bounds, unknown or unsupported fields, response classification, and idempotency/reconciliation behavior where applicable. Use mocks; never call production services or require live credentials.
4. Implement the smallest typed change that preserves all supported fields and rejects unsupported behavior explicitly. Do not silently discard fields, invent a fallback, or treat advisory results as authorization.
5. Update contract documentation, the compatibility inventory's evidence map, architecture diagrams or failure matrices, and the implementation roadmap as applicable. Record intentional differences from Kaseki and mark future or unwired behavior as deferred.
6. Run the relevant checks from the [contributing guide](../../../CONTRIBUTING.md). Report which fixtures and checks were exercised and any integration evidence still outstanding.

## Review questions

- Is the source of truth current and identified?
- Are versioning, required fields, limits, identity, and error semantics explicit?
- Can an uncertain response be reconciled without changing the logical operation ID?
- Are tests independent of production tokens, public network access, and external infrastructure?
- Do docs distinguish tested behavior from live integration or production rollout?

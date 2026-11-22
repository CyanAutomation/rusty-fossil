---
name: coding-worker-runtime
description: Use only when implementing the planned Phase 2C coding-agent runtime or its explicitly requested design; do not use for the current synthetic-only Phase 2B worker.
---

# Planned coding-worker runtime

This skill applies only when Phase 2C work is explicitly requested. The current worker is synthetic-only and has no repository checkout, Pi CLI, Git operation, or publication capability. Start by checking the [implementation plan](../../../docs/IMPLEMENTATION-PLAN.md), [architecture](../../../docs/ARCHITECTURE.md), and [Phase 2B limitations](../../../docs/PHASE-2B-OPERATIONS.md#limitations-before-phase-2c); do not treat this skill as evidence that Phase 2C exists.

## Workflow

1. Define the runtime boundary before implementation: repository workspace, allowed tools and commands, network access, mounted paths, secrets, resource limits, output capture, and cleanup. Keep all authority inside the proof-gated supervisor and preserve its canonical Soyuz checks.
2. Use a disposable repository and non-production credentials for development and integration work. Keep secrets out of prompts, source trees, artifacts, logs, SQLite, and model-visible output unless a separately reviewed design explicitly requires a scoped secret path.
3. Treat prompts, model output, repository files, process output, validation results, and generated artifacts as untrusted. Bound sizes and execution time, redact sensitive data, and validate outputs independently before accepting them.
4. Add contract and lifecycle tests with mocks first. Test isolation boundaries, cancellation, timeout, restart recovery, malformed output, and cleanup. Do not make normal tests depend on a live model, production service, public network, or developer credentials.
5. Keep repository preparation and artifact handling inside the worker boundary. Do not add GitHub publication or production credentials as part of Phase 2C; publication belongs to the separately planned Phase 3 and requires its own explicit authorization and review.
6. Update architecture, contracts, compatibility evidence, the implementation plan, and operations guidance. State what is mocked, what is integration-tested, and what remains disabled in the production service.

# ADR 0002: Keep controller recovery state in SQLite

- Status: accepted
- Date: 2026-11-18

## Context

The first controller runs one execution slot per host. It needs atomic local claim-intent and start-authorization updates, restart recovery, unique run and callback IDs, and a small durable outbox. Soyuz remains the shared source of truth across hosts.

## Decision

Use a migration-managed SQLite database in the configured durable state directory. Enable WAL, a busy timeout, foreign-key checks, transactional compare-and-set transitions, and explicit schema-version rejection. Bundle SQLite through `rusqlite` so the runtime does not depend on a host library. Persist only Soyuz-owned run identity and bounded recovery/callback metadata.

## Suitability assessment

SQLite is embedded, transactional, and adequate for one process with one active execution slot. It avoids a network database dependency while preserving crash-safe state updates. Separate hosts do not share the file; they coordinate through Soyuz's claim compare-and-set and fencing token. SQLite therefore never becomes a distributed lock or task queue.

## Rejected alternatives

- A JSON state file: no transactions or database-enforced uniqueness for callback/claim IDs.
- Postgres or another network database: adds an operational dependency without a Phase 1 multi-host shared-state requirement.
- In-memory state: loses claim intent, authorization, and callback recovery on restart.
- Importing Kaseki state: couples a clean implementation to legacy formats and is explicitly outside this phase.

## Consequences

The initial schema is small and local. Corrupt or newer schemas fail closed instead of being overwritten. A future multi-controller shared coordinator would need a separate architectural decision; it must not weaken Soyuz's authority.

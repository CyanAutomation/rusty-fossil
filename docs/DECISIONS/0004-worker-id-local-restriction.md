# ADR 0004: Keep WorkerId restricted to host-safe ASCII

- Status: accepted
- Date: 2026-11-19

## Context

Soyuz's shared worker callback contract permits a wider string than Rusty-Fossil needs. Rusty-Fossil persists a stable host ID, binds it into claim and callback fences, and may later use it in local diagnostics and operational tooling. Relaxing the local rule would create avoidable parsing and logging ambiguity without a compatibility need for this single-host controller.

## Decision

Keep `WorkerId` to 1–64 ASCII alphanumeric characters, hyphen, and underscore. Reject periods, colons, whitespace, Unicode and empty values at ID parsing and configuration loading. Do not silently truncate or normalize identifiers.

## Consequences

This is intentionally stricter than Soyuz's broader contract. Operators must choose a stable host identifier within the local restriction. Tests cover the 64-character accepted boundary, the 65-character rejected boundary, and rejected punctuation.

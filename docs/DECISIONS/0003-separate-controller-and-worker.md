# ADR 0003: Separate the native controller from disposable workers

- Status: accepted
- Date: 2026-11-18

## Context

The controller needs persistent control-plane credentials and local state. Coding tasks need access to untrusted repositories, a model gateway, and build tools. Combining those privileges in one process or image increases the impact of a compromised task.

## Decision

Build the controller as a small Rust service with Tokio, Axum diagnostics, typed Serde contracts, Reqwest with Rustls, structured tracing, and SQLite persistence. Package it independently from a future disposable worker image. Phase 1 does not mount the Docker socket, install a coding runtime, clone repositories, or launch processes.

Future worker execution will use a single active slot initially. A worker image will run separately as non-root with bounded CPU, memory, time, filesystem, and network access. The controller's Docker supervisor may be introduced only after Soyuz authorization is durably proven and the worker isolation policy is implemented.

## Rejected alternatives

- One image with controller, Docker Engine, Node, Pi CLI, and compilers: expands the controller's supply chain and privilege surface.
- Actor framework or custom workflow engine: no demonstrated requirement beyond one active slot and durable records.
- Direct shell command construction: unnecessary in Phase 1 and unsafe for untrusted task input.

## Consequences

Phase 1 produces a runnable native diagnostics service and testable boundaries, not live execution. Later execution code must have a narrow launch interface that cannot be reached without a Soyuz authorization proof.

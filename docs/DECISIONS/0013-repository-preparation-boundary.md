# ADR 0013: Keep repository preparation behind a trusted host boundary

**Status:** Accepted for Phase 2C-A groundwork
**Date:** 2026-11-26

## Context

Pi execution needs a repository checkout, but the current supervisor supports only the synthetic workload. Queue input cannot be allowed to select arbitrary Git hosts, checkout paths, Docker mounts, helpers, or Git behavior. The task prompt is untrusted content and must not select repository policy. A coding worker also must not receive provider, Soyuz, GitHub, Cloudflare, Senku, SSH, or Docker credentials.

The durable launch identity already records the run's repository URL, authorized Git ref, resolved commit and opaque workspace identity. It does not implement the trusted checkout or permit Pi launch.

## Decision

Implement repository preparation as a separate Rust library with no Docker dependency. Its trusted caller supplies an exact operator-owned HTTPS URL allowlist and a private workspace root. The initial pilot policy permits only `https://github.com/CyanAutomation/somatic`.

The library:

- Authorizes only canonical patch requests with `publishMode: none` and a non-empty persisted prompt; it inspects prompt presence only.
- Accepts branch refs only, resolves one exact advertised commit, and requires recovery identities to match the canonical Soyuz request.
- Runs Git with system/global config, helper, hooks, redirects, submodules, LFS smudge and non-HTTPS protocols disabled.
- Routes Git HTTPS through a short-lived CONNECT proxy that permits only the approved hostname on port 443 and dials filtered public addresses.
- Checks the fetched SHA against the persisted SHA before checkout, validates tree modes and paths, rejects symlinks and gitlinks, and bounds object/tree/checkout sizes.
- Uses a unique opaque workspace identity and limits preparation and cleanup to a direct child of a private configured root.

Integration must persist the resolved commit and workspace identity durably before checkout or any Docker side effect. Agentlab's owner expects both host containers to run as the existing `pi` account, so this deployment will not provision a separate repository-preparation account. Repository preparation must run in the controller container with no Docker socket mount, no Docker group and no alternate Docker API endpoint. Because the controller and supervisor share the Unix identity, this is process confinement rather than a distinct-account privilege boundary; the interactive `pi` account remains rootful-Docker capable. Do not enable repository preparation until the Compose socket boundary and Docker listener policy are verified on the host. Only the exact prepared workspace may later be mounted read/write into a matching worker; no other host path is permitted.

This library does not enforce model egress, broker inference, provide durable prompt delivery, or admit a Pi container. The supervisor continues to reject Pi workload records. The Pi worker foundation now reads a fixed-path read-only payload file, but supervisor staging and mounting are not implemented. The Git proxy restricts this process's Git connections only; it is not the worker's network isolation mechanism.

## Consequences

- Policy and parsing can be tested without Docker or provider credentials.
- A branch moving between resolution and fetch fails closed rather than silently changing the task input.
- Repository size and path features are deliberately restricted; repositories requiring symlinks, submodules, Git LFS, large files, or non-UTF-8 paths are not supported by this pilot.
- No live remote checkout or native ARM64 execution has been validated yet.
- Before supervisor integration, add a durable storage seam that prevents checkout from preceding commit/workspace identity persistence and prove crash/cancellation cleanup semantics.
- No host-level account or permission changes were performed. The proposed shared-`pi` Compose process boundary still requires deployment review and on-host verification.

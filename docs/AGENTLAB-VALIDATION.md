# Agentlab validation record

**Updated:** 2026-11-27
**Phase:** 2C-A.3
**Outcome:** Read-only preflight completed; canary not deployed

## Checks performed

| Area | Observation |
| --- | --- |
| Native platform and capacity | ARM64 Raspberry Pi 4-class host, four CPUs, about 3.2 GiB memory available, 2 GiB swap, and about 34 GiB free root storage at inspection time. |
| Docker | Engine 29.2.9, Compose 5.6.1, cgroup v2, and built-in seccomp support reported. Effective Pi container policy was not tested because no worker was created. |
| Existing workloads | Three containers were running. No Rusty-Fossil Compose project container was found. No workload was changed. |
| Docker API exposure | A candidate Docker Engine TCP listener was observed bound beyond loopback. The operator confirmed this listener is expected. No connection or reachability test was made; transport, authentication and effective reachability were not independently verified. |
| Repository | No Somatic ref was resolved or fetched; no workspace was created. |
| Inference | No provider request was sent and no cost was incurred. |
| Lifecycle | No Pi container, callback, cancellation, restart recovery, or cleanup flow was exercised. |

## Cloudflare AI Gateway check

Cloudflare API reads found one default AI Gateway. Its configuration reported request logging enabled, zero data retention disabled, no gateway spend rules, and three configured upstream retry attempts. No Cloudflare setting, credential, or billing configuration was changed. Rusty-Fossil now disables payload collection per request and correlates inference events with gateway logs for cost reporting; the account-level logging/retention configuration is unchanged.

Rusty-Fossil records Cloudflare-reported estimated cost and exposes a run-fenced report for Soyuz cost-per-PR accounting. It has no per-run spending, inference-count, request-rate, or inference-token budget. The observed estimate is telemetry and does not establish a billing ceiling. This branch did not send an inference request.

## Disposition and prerequisites

The canary was not deployed. The listener is an expected host configuration per operator confirmation and is not treated as a blocker for this work. Remaining software and inference gates are:

1. An approved data-handling route for repository content and task prompts has not been established.
2. Controller/supervisor runtime integration, Pi launch profile, repository preparation, broker capability issuance and revocation, enforced worker egress, and complete Pi lifecycle recovery are not implemented.

No attempt was made to bypass those gates. Production Queue polling remains disabled.

No host firewall, Docker daemon, account, Cloudflare gateway, provider credential, or unrelated workload was changed. Do not treat this preflight as a simulator-backed run or as native task validation.

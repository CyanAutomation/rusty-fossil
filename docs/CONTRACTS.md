# Contract overview

This document describes the compatibility rules for versioned data contracts used by this workspace.

## Versioning

- Validate input against the schema for its declared version.
- Preserve documented identifiers and optional fields when normalizing a message.
- Reject unknown contract versions and unsupported fields rather than silently reinterpreting them.
- Add a new version when a change would make existing input or output incompatible.

The Senku v1 response envelope is an additive-reader exception: the client validates every required top-level field, ignores unknown top-level fields, and accepts any positive `attemptCount`. Profile answer IDs, answer object fields, answer values, and the versioned request input remain strict. This lets Senku add response metadata without weakening validation of decision content.

Rusty-Fossil pins Senku's generated profile contract artifact at `crates/fossil-senku/contracts/senku-profiles.v1.json`. The root `.senku-contract.sha256` file records the exact artifact digest and source Senku revision; CI verifies the checked-in artifact against that digest. Update the artifact and pin together as a reviewed contract change.

## Fixtures and tests

Checked-in fixtures provide representative contract examples. Add tests for required fields, supported variants, normalization, and compatibility whenever a schema changes.

Environment-specific configuration and release procedures are maintained separately from this contract reference.

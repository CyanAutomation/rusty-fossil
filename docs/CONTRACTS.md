# Contract overview

This document describes the compatibility rules for versioned data contracts used by this workspace.

## Versioning

- Validate input against the schema for its declared version.
- Preserve documented identifiers and optional fields when normalizing a message.
- Reject unknown contract versions and unsupported fields rather than silently reinterpreting them.
- Add a new version when a change would make existing input or output incompatible.

## Fixtures and tests

Checked-in fixtures provide representative contract examples. Add tests for required fields, supported variants, normalization, and compatibility whenever a schema changes.

Environment-specific configuration and release procedures are maintained separately from this contract reference.

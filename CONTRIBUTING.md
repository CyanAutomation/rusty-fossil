# Contributing

Rusty-Fossil is a clean-room Rust controller. Soyuz remains the only authority for run creation, identity, state, claims, and start authorization. The Phase 1 binary is diagnostics-only; do not add a local run source, standalone scheduler, submission route, or production Queue consumption.

## Development

Install the stable Rust toolchain. The declared minimum is Rust 1.89. `rust-toolchain.toml` selects stable with `rustfmt` and Clippy. Use the lockfile for builds.

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --release
```

Write the contract or lifecycle test first, then implement the smallest behavior that makes it pass. Use recorded JSON fixtures and mock HTTP services; tests must not need production tokens, public network access, Soyuz infrastructure, or Docker. Include applicable Soyuz requirement IDs in test comments and the compatibility inventory. Clearly mark behavior requiring a later worker or production consumer as deferred.

## Security and state

- Never log raw requests, prompts, repository data, credentials, callback bodies, or Queue messages.
- Do not put secrets in SQLite. Keep mounted secret files preferred; environment values are for local tests.
- Preserve distinct canonical run, display, Queue message, Queue lease, worker, claim-fence, and callback identifiers.
- Keep local records tied to a Soyuz run. Start authorization must come from the `fossil-soyuz` proof type and match the persisted run, worker, and claim.
- Do not add implicit retries for non-idempotent operations. Reuse durable callback and event IDs for retries.
- Do not mount Docker or add Docker privileges to the Phase 1 controller.
- Keep diagnostics read-only and bound to loopback by default.

## Documentation

Update the architecture, protocol notes, compatibility inventory, and implementation roadmap when behavior or external contracts change. Soyuz's current published contract takes precedence for Soyuz-facing behavior. Record differences from Kaseki as intentional compatibility decisions.

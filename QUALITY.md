# Local Quality Checks

This repository uses a local quality gate instead of CI. Run it before committing changes:

```bash
./scripts/check.sh
```

The gate checks both normal tokio/tonic builds and the `cfg(madsim)` simulation build. This matters because the project has target-specific dependencies and generated tonic code for both runtimes.

## Required Tools

Install the Rust toolchain components and cargo subcommands:

```bash
rustup component add rustfmt clippy
cargo install cargo-deny cargo-audit cargo-machete
```

`scripts/check.sh` does not install tools automatically. If a required cargo subcommand is missing, the script prints the install command and exits.

## Checks

The local gate runs:

```bash
cargo fmt --check
cargo check --all-targets
RUSTFLAGS="--cfg madsim" cargo check --all-targets
cargo clippy --all-targets -- -D warnings
RUSTFLAGS="--cfg madsim" cargo clippy --all-targets -- -D warnings
cargo test
RUSTFLAGS="--cfg madsim" cargo test
cargo machete
cargo deny check
cargo audit --deny warnings
```

`cargo machete` ignores `bytes` and `prost` because they are referenced by generated tonic/prost code rather than explicit handwritten source.

#!/usr/bin/env bash
set -euo pipefail

require_cargo_subcommand() {
    local subcommand="$1"
    local package="$2"

    if ! cargo "${subcommand}" --version >/dev/null 2>&1; then
        echo "missing cargo-${subcommand}; install it with: cargo install ${package}" >&2
        exit 127
    fi
}

run() {
    echo
    echo "+ $*"
    "$@"
}

require_cargo_subcommand machete cargo-machete
require_cargo_subcommand deny cargo-deny
require_cargo_subcommand audit cargo-audit

run cargo fmt --check
run cargo check --all-targets
run env RUSTFLAGS="--cfg madsim" cargo check --all-targets
run cargo clippy --all-targets -- -D warnings
run env RUSTFLAGS="--cfg madsim" cargo clippy --all-targets -- -D warnings
run cargo test
run env RUSTFLAGS="--cfg madsim" cargo test
run cargo machete
run cargo deny check
run cargo audit --deny warnings

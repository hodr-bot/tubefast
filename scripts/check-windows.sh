#!/usr/bin/env bash
# Cross-target clippy gate for the tubefast fork: proves the cfg-gated
# code compiles warning-free for Windows and macOS before pushing.
# Usage: wincheck.sh <rustup-bin> [target...]
set -u
RUSTUP_BIN="${1:?rustup binary path required}"
shift
TARGETS=("$@")
if [ "${#TARGETS[@]}" -eq 0 ]; then
    TARGETS=(x86_64-pc-windows-gnu aarch64-apple-darwin)
fi
export CARGO_HOME=$HOME/.cargo-check
export RUSTUP_HOME=$HOME/.rustup-check
export PATH="$CARGO_HOME/bin:$HOME/.nix-profile/bin:/nix/var/nix/profiles/default/bin:/usr/bin:/bin"
TOOLCHAIN="${TUBEFAST_TOOLCHAIN:-1.90}"
"$RUSTUP_BIN" toolchain install "$TOOLCHAIN" --profile minimal --no-self-update
"$RUSTUP_BIN" default "$TOOLCHAIN"
for t in "${TARGETS[@]}"; do
    "$RUSTUP_BIN" target add --toolchain "$TOOLCHAIN" "$t"
done
"$RUSTUP_BIN" component add --toolchain "$TOOLCHAIN" clippy
# The nix rustup build creates no CARGO_HOME shims: call the toolchain
# binaries directly.
TC_BIN=($HOME/.rustup-check/toolchains/*/bin)
CARGO_RUN=("${TC_BIN[0]}/cargo")
# cargo discovers external subcommands (cargo-clippy) via PATH.
export PATH="${TC_BIN[0]}:$PATH"
# Optional cross C toolchains (passed via env): MINGW_BIN for
# x86_64-pc-windows-gnu, DARWIN_BIN for aarch64-apple-darwin.
if [ -n "${MINGW_BIN:-}" ]; then
    export PATH="$MINGW_BIN:$PATH"
    export CC_x86_64_pc_windows_gnu="$MINGW_BIN/x86_64-w64-mingw32-gcc"
    export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="$MINGW_BIN/x86_64-w64-mingw32-gcc"
fi
cd "$(dirname "$0")/.." || exit 1
FAIL=0
for t in "${TARGETS[@]}"; do
    echo "=== clippy $t ==="
    RUSTFLAGS="-D warnings" "${CARGO_RUN[@]}" clippy --locked --all-targets --target "$t" 2>&1 | tail -25
    # shellcheck disable=SC2181
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        echo "CLIPPY-FAIL $t"
        FAIL=1
    else
        echo "CLIPPY-OK $t"
    fi
done
exit "$FAIL"

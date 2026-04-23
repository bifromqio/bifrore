#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  echo "Usage: ./scripts/bench.sh [rust [filter]|jmh [jmh-args...]]"
  exit 1
}

MODE="${1:-rust}"
shift || true

run_rust_bench() {
  echo "Running Rust benchmarks..."
  if [[ $# -gt 0 ]]; then
    (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core -- "$1")
  else
    (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core)
  fi
}

run_java_bench() {
  echo "Running Java JMH benchmarks..."
  build_rust
  build_jni
  build_java_jar
  run_java_jmh "$@"
}

case "$MODE" in
  rust)
    run_rust_bench "$@"
    ;;
  jmh)
    run_java_bench "$@"
    ;;
  *)
    usage
    ;;
esac

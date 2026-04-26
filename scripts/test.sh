#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  echo "Usage: ./scripts/test.sh [core|jni|java-integration|all]"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

TARGET="$1"

run_core_tests() {
  echo "Running Rust core tests..."
  (cd "$RUST_DIR" && cargo test -p bifrore-embed-core --features mqtt -- --nocapture)
}

run_jni_tests() {
  build_jni_test
}

run_java_integration_tests() {
  echo "Running JNI integration tests..."
  local maven_repo_local="${MAVEN_REPO_LOCAL:-$ROOT_DIR/.m2-test}"
  build_rust
  build_jni
  build_java_jar
  install_java_jar_to_local_maven "$maven_repo_local"
  (cd "$ROOT_DIR" && mvn -q -Dmaven.repo.local="$maven_repo_local" -Dbifrore.java.version="$BIFRORE_VERSION" -f bindings/jni/test/pom.xml test)
}

case "$TARGET" in
  core)
    run_core_tests
    ;;
  jni)
    run_jni_tests
    ;;
  java-integration)
    run_java_integration_tests
    ;;
  all)
    run_core_tests
    run_jni_tests
    run_java_integration_tests
    ;;
  *)
    usage
    ;;
esac

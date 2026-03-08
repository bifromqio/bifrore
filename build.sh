#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
RUST_DIR="$ROOT_DIR/engine"

usage() {
  echo "Usage: ./build.sh [jni|python|provision-cli|all|bench|bench-diff]"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

TARGET="$1"

OS_NAME="$(uname -s)"
case "$OS_NAME" in
  Darwin)
    RUST_LIB_EXT="dylib"
    JNI_LIB_EXT="dylib"
    JNI_CFLAGS="-fPIC"
    JNI_LDFLAGS="-dynamiclib -Wl,-rpath,@loader_path"
    ;;
  Linux)
    RUST_LIB_EXT="so"
    JNI_LIB_EXT="so"
    JNI_CFLAGS="-fPIC"
    JNI_LDFLAGS="-shared -Wl,-rpath,'$$ORIGIN'"
    ;;
  *)
    echo "Unsupported OS: $OS_NAME"
    exit 2
    ;;
esac

mkdir -p "$BUILD_DIR"

build_rust() {
  echo "Building Rust cdylib..."
  (cd "$RUST_DIR" && cargo build --release -p bifrore-embed-ffi --features mqtt)
  cp "$RUST_DIR/target/release/libbifrore_embed.$RUST_LIB_EXT" "$BUILD_DIR/"
}

build_jni() {
  echo "Building JNI bridge..."
  if [[ -z "${JAVA_HOME:-}" ]]; then
    if [[ "$OS_NAME" == "Darwin" ]]; then
      JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
    fi
  fi
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "JAVA_HOME is not set"
    exit 3
  fi
  JNI_INCLUDE="$JAVA_HOME/include"
  JNI_PLATFORM_INCLUDE="$JNI_INCLUDE"
  if [[ "$OS_NAME" == "Darwin" ]]; then
    JNI_PLATFORM_INCLUDE="$JNI_INCLUDE/darwin"
  elif [[ "$OS_NAME" == "Linux" ]]; then
    JNI_PLATFORM_INCLUDE="$JNI_INCLUDE/linux"
  fi

  cc $JNI_CFLAGS \
    -I"$JNI_INCLUDE" -I"$JNI_PLATFORM_INCLUDE" \
    -L"$BUILD_DIR" -lbifrore_embed \
    $JNI_LDFLAGS \
    -o "$BUILD_DIR/libbifrore_jni.$JNI_LIB_EXT" \
    "$ROOT_DIR/bindings/jni/src/main/c/bifrore_jni.c"
}

build_python() {
  echo "Python wrapper is ready at bindings/python/bifrore.py"
}

build_provision_cli() {
  echo "Building client-id provision CLI..."
  (cd "$RUST_DIR" && cargo build --release -p bifrore-embed-ffi --bin bifrore-clientid-provision)
  cp "$RUST_DIR/target/release/bifrore-clientid-provision" "$BUILD_DIR/"
}

run_bench() {
  echo "Running benchmarks..."
  (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core)
}

run_bench_diff() {
  echo "Running parse-only benchmark diff: serde_json vs simd-json..."
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap "rm -rf '$tmp_dir'" EXIT

  local serde_dir="$tmp_dir/criterion_serde"
  local simd_dir="$tmp_dir/criterion_simd"

  (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core --bench runtime_bench parse_only)
  cp -R "$RUST_DIR/target/criterion" "$serde_dir"

  (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core --bench runtime_bench --features simd-json parse_only)
  cp -R "$RUST_DIR/target/criterion" "$simd_dir"

  node -e '
const fs = require("fs");
const path = require("path");

function readBenchNs(baseDir) {
  const result = {};
  if (!fs.existsSync(baseDir)) return result;
  for (const entry of fs.readdirSync(baseDir)) {
    const estimatePath = path.join(baseDir, entry, "new", "estimates.json");
    if (!fs.existsSync(estimatePath)) continue;
    const json = JSON.parse(fs.readFileSync(estimatePath, "utf8"));
    if (json.mean && typeof json.mean.point_estimate === "number") {
      result[entry] = json.mean.point_estimate;
    }
  }
  return result;
}

function fmtNs(ns) {
  if (ns < 1000) return `${ns.toFixed(3)} ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(3)} µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(3)} ms`;
  return `${(ns / 1_000_000_000).toFixed(3)} s`;
}

function pctDelta(newVal, oldVal) {
  if (!oldVal) return 0;
  return ((newVal - oldVal) / oldVal) * 100;
}

const serdeBase = process.argv[1];
const simdBase = process.argv[2];
const serde = readBenchNs(serdeBase);
const simd = readBenchNs(simdBase);

console.log("");
console.log("=== Parse-only JSON parser diff (smaller is better) ===");
const jsonNames = Object.keys(simd)
  .filter((name) => name.startsWith("parse_only_") && name.endsWith("_json") && serde[name] !== undefined)
  .sort();
if (jsonNames.length === 0) {
  console.log("No matching JSON benchmarks found.");
} else {
  for (const name of jsonNames) {
    const s = serde[name];
    const m = simd[name];
    const d = pctDelta(m, s);
    console.log(
      `${name.padEnd(38)} serde=${fmtNs(s).padEnd(14)} simd=${fmtNs(m).padEnd(14)} delta=${d.toFixed(2).padStart(8)}%`
    );
  }
}

console.log("");
console.log("=== Parse-only payload diff inside simd-json run ===");
const protobufNames = Object.keys(simd)
  .filter((name) => name.startsWith("parse_only_") && name.endsWith("_protobuf"))
  .sort();
if (protobufNames.length === 0) {
  console.log("No parse-only protobuf benchmarks found in simd-json run.");
} else {
  let printed = 0;
  for (const pbName of protobufNames) {
    const jsonName = pbName.replace(/_protobuf$/, "_json");
    if (simd[jsonName] === undefined) continue;
    const d = pctDelta(simd[pbName], simd[jsonName]);
    const label = pbName.replace(/^parse_only_/, "").replace(/_protobuf$/, "");
    console.log(
      `${label.padEnd(38)} json=${fmtNs(simd[jsonName]).padEnd(14)} protobuf=${fmtNs(simd[pbName]).padEnd(14)} delta=${d.toFixed(2).padStart(8)}%`
    );
    printed += 1;
  }
  if (printed === 0) {
    console.log("No matching parse_only_*_json benchmark found for parse_only_*_protobuf cases.");
  }
}
' "$serde_dir" "$simd_dir"
}

case "$TARGET" in
  jni)
    build_rust
    build_jni
    ;;
  python)
    build_rust
    build_python
    ;;
  provision-cli)
    build_provision_cli
    ;;
  all)
    build_rust
    build_jni
    build_python
    build_provision_cli
    ;;
  bench)
    run_bench
    ;;
  bench-diff)
    run_bench_diff
    ;;
  *)
    usage
    ;;
esac

echo "Build artifacts are in $BUILD_DIR"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
RUST_DIR="$ROOT_DIR/engine"

usage() {
  echo "Usage: ./build.sh [java|jni|jni-test|python|all|bench|bench-diff] [benchmark_name]"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

TARGET="$1"
BENCH_FILTER="${2:-}"

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
    JNI_LDFLAGS='-shared -Wl,-rpath,$ORIGIN'
    ;;
  *)
    echo "Unsupported OS: $OS_NAME"
    exit 2
    ;;
esac

mkdir -p "$BUILD_DIR"

platform_tag() {
  if [[ -n "${BIFRORE_WHEEL_PLATFORM_TAG:-}" ]]; then
    echo "$BIFRORE_WHEEL_PLATFORM_TAG"
    return
  fi
  case "$OS_NAME" in
    Darwin)
      case "$(uname -m)" in
        arm64|aarch64) echo "macosx_11_0_arm64" ;;
        *) echo "unsupported" ;;
      esac
      ;;
    Linux)
      case "$(uname -m)" in
        x86_64|amd64) echo "manylinux2014_x86_64" ;;
        *) echo "unsupported" ;;
      esac
      ;;
    *)
      echo "unsupported"
      ;;
  esac
}

native_platform_dir() {
  case "$OS_NAME" in
    Darwin)
      case "$(uname -m)" in
        arm64|aarch64) echo "darwin-aarch64" ;;
        *) echo "unsupported" ;;
      esac
      ;;
    Linux)
      case "$(uname -m)" in
        x86_64|amd64) echo "linux-x86_64" ;;
        *) echo "unsupported" ;;
      esac
      ;;
    *)
      echo "unsupported"
      ;;
  esac
}

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
    -o "$BUILD_DIR/libbifrore_jni.$JNI_LIB_EXT" \
    "$ROOT_DIR/bindings/jni/src/main/c/bifrore_jni.c" \
    -L"$BUILD_DIR" -lbifrore_embed \
    $JNI_LDFLAGS
}

build_jni_test() {
  echo "Building JNI direct-helper tests..."
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

  cc $JNI_CFLAGS -pthread \
    -I"$JNI_INCLUDE" -I"$JNI_PLATFORM_INCLUDE" \
    -o "$BUILD_DIR/bifrore-jni-direct-test" \
    "$ROOT_DIR/bindings/jni/test/c/bifrore_jni_direct_test.c"

  "$BUILD_DIR/bifrore-jni-direct-test"
}

build_java_jar() {
  echo "Packaging Java jar..."
  local native_dir platform classes_dir resources_root resources_dir jar_name
  platform="$(native_platform_dir)"
  if [[ "$platform" == "unsupported" ]]; then
    echo "Unsupported platform for Java packaging: $OS_NAME/$(uname -m)"
    exit 4
  fi
  native_dir="$BUILD_DIR/java-stage/natives/$platform"
  classes_dir="$BUILD_DIR/java-stage/classes"
  resources_root="$BUILD_DIR/java-stage/resources"
  resources_dir="$resources_root/natives"
  jar_name="bifrore-java.jar"
  rm -f "$BUILD_DIR"/bifrore-java*.jar
  rm -rf "$BUILD_DIR/java-stage"
  mkdir -p "$native_dir" "$classes_dir" "$resources_dir"

  cp "$BUILD_DIR/libbifrore_embed.$RUST_LIB_EXT" "$native_dir/"
  cp "$BUILD_DIR/libbifrore_jni.$JNI_LIB_EXT" "$native_dir/"

  find "$ROOT_DIR/bindings/jni/src/main/java" -name '*.java' -print0 | \
    xargs -0 javac --release 17 -d "$classes_dir"

  cp -R "$native_dir" "$resources_dir/"

  (cd "$BUILD_DIR/java-stage" && jar --create --file "$BUILD_DIR/$jar_name" -C "$classes_dir" . -C "$resources_root" .)
}

build_python() {
  echo "Building Python wheel..."
  local platform wheel_stage package_dir native_dir dist_dir
  platform="$(platform_tag)"
  if [[ "$platform" == "unsupported" ]]; then
    echo "Unsupported platform for Python wheel: $OS_NAME/$(uname -m)"
    exit 5
  fi
  wheel_stage="$BUILD_DIR/python-wheel-stage"
  package_dir="$wheel_stage/bifrore"
  native_dir="$package_dir"
  dist_dir="$BUILD_DIR"
  rm -f "$BUILD_DIR"/bifrore-*.whl
  rm -rf "$wheel_stage"
  mkdir -p "$package_dir"

  cp "$ROOT_DIR/bindings/python/bifrore.py" "$package_dir/__init__.py"
  cp "$BUILD_DIR/libbifrore_embed.$RUST_LIB_EXT" "$native_dir/"

  cat > "$wheel_stage/setup.py" <<'EOF'
from setuptools import Distribution, setup


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(
    name="bifrore",
    version="0.1.0",
    description="BifroRE Python bindings",
    packages=["bifrore"],
    package_data={"bifrore": ["libbifrore_embed.*"]},
    include_package_data=True,
    distclass=BinaryDistribution,
)
EOF

  (cd "$wheel_stage" && python3 setup.py bdist_wheel --plat-name "$platform" --dist-dir "$dist_dir")
}

run_bench() {
  echo "Running benchmarks..."
  if [[ -n "$BENCH_FILTER" ]]; then
    (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core --bench runtime_bench -- "$BENCH_FILTER")
  else
    (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core)
  fi
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
  jni-test)
    build_jni_test
    ;;
  java)
    build_rust
    build_jni
    build_java_jar
    ;;
  python)
    build_rust
    build_python
    ;;
  all)
    build_rust
    build_jni
    build_java_jar
    build_python
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

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
RUST_DIR="$ROOT_DIR/engine"

usage() {
  echo "Usage: ./build.sh [jni|python|all|bench]"
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

run_bench() {
  echo "Running benchmarks..."
  (cd "$RUST_DIR" && cargo bench -p bifrore-embed-core)
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
  all)
    build_rust
    build_jni
    build_python
    ;;
  bench)
    run_bench
    ;;
  *)
    usage
    ;;
esac

echo "Build artifacts are in $BUILD_DIR"

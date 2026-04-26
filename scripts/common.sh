#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
RUST_DIR="$ROOT_DIR/engine"
BIFRORE_VERSION="${BIFRORE_VERSION:-0.1.0}"

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

ensure_java_home() {
  if [[ -z "${JAVA_HOME:-}" ]]; then
    if [[ "$OS_NAME" == "Darwin" ]]; then
      JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
    fi
  fi
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "JAVA_HOME is not set"
    exit 3
  fi
}

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

java_jar_name() {
  local platform
  platform="$(native_platform_dir)"
  if [[ "$platform" == "unsupported" ]]; then
    echo "unsupported"
    return
  fi
  echo "bifrore-${BIFRORE_VERSION}-${platform}.jar"
}

java_jar_path() {
  local jar_name
  jar_name="$(java_jar_name)"
  if [[ "$jar_name" == "unsupported" ]]; then
    echo "unsupported"
    return
  fi
  echo "$BUILD_DIR/$jar_name"
}

build_rust() {
  echo "Building Rust cdylib..."
  (cd "$RUST_DIR" && cargo build --release -p bifrore-embed-ffi --features mqtt)
  cp "$RUST_DIR/target/release/libbifrore_embed.$RUST_LIB_EXT" "$BUILD_DIR/"
}

build_jni() {
  echo "Building JNI bridge..."
  ensure_java_home
  local jni_include jni_platform_include
  jni_include="$JAVA_HOME/include"
  jni_platform_include="$jni_include"
  if [[ "$OS_NAME" == "Darwin" ]]; then
    jni_platform_include="$jni_include/darwin"
  elif [[ "$OS_NAME" == "Linux" ]]; then
    jni_platform_include="$jni_include/linux"
  fi

  cc $JNI_CFLAGS \
    -I"$jni_include" -I"$jni_platform_include" \
    -o "$BUILD_DIR/libbifrore_jni.$JNI_LIB_EXT" \
    "$ROOT_DIR/bindings/jni/src/main/c/bifrore_jni.c" \
    "$ROOT_DIR/bindings/jni/src/main/c/bifrore_jni_heap_poll.c" \
    "$ROOT_DIR/bindings/jni/src/main/c/bifrore_jni_direct_poll.c" \
    -L"$BUILD_DIR" -lbifrore_embed \
    $JNI_LDFLAGS
}

build_jni_test() {
  echo "Building JNI direct-helper tests..."
  ensure_java_home
  local jni_include jni_platform_include
  jni_include="$JAVA_HOME/include"
  jni_platform_include="$jni_include"
  if [[ "$OS_NAME" == "Darwin" ]]; then
    jni_platform_include="$jni_include/darwin"
  elif [[ "$OS_NAME" == "Linux" ]]; then
    jni_platform_include="$jni_include/linux"
  fi

  cc $JNI_CFLAGS -pthread \
    -I"$jni_include" -I"$jni_platform_include" \
    -o "$BUILD_DIR/bifrore-jni-direct-test" \
    "$ROOT_DIR/bindings/jni/test/c/bifrore_jni_direct_test.c"

  "$BUILD_DIR/bifrore-jni-direct-test"
}

build_java_jar() {
  echo "Packaging Java jar..."
  local native_dir platform classes_dir resources_root resources_dir jar_path
  platform="$(native_platform_dir)"
  if [[ "$platform" == "unsupported" ]]; then
    echo "Unsupported platform for Java packaging: $OS_NAME/$(uname -m)"
    exit 4
  fi
  native_dir="$BUILD_DIR/java-stage/natives/$platform"
  classes_dir="$BUILD_DIR/java-stage/classes"
  resources_root="$BUILD_DIR/java-stage/resources"
  resources_dir="$resources_root/natives"
  jar_path="$(java_jar_path)"
  rm -f "$BUILD_DIR"/bifrore-*.jar
  rm -rf "$BUILD_DIR/java-stage"
  mkdir -p "$native_dir" "$classes_dir" "$resources_dir"

  cp "$BUILD_DIR/libbifrore_embed.$RUST_LIB_EXT" "$native_dir/"
  cp "$BUILD_DIR/libbifrore_jni.$JNI_LIB_EXT" "$native_dir/"

  find "$ROOT_DIR/bindings/jni/src/main/java" -name '*.java' -print0 | \
    xargs -0 javac --release 17 -d "$classes_dir"

  cp -R "$native_dir" "$resources_dir/"

  (cd "$BUILD_DIR/java-stage" && jar --create --file "$jar_path" -C "$classes_dir" . -C "$resources_root" .)
  echo "Java jar: $jar_path"
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

  cat > "$wheel_stage/setup.py" <<EOF
from setuptools import Distribution, setup


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(
    name="bifrore",
    version="${BIFRORE_VERSION}",
    description="BifroRE Python bindings",
    packages=["bifrore"],
    package_data={"bifrore": ["libbifrore_embed.*"]},
    include_package_data=True,
    distclass=BinaryDistribution,
)
EOF

  (cd "$wheel_stage" && python3 setup.py bdist_wheel --plat-name "$platform" --dist-dir "$dist_dir")
}

install_java_jar_to_local_maven() {
  local maven_repo_local="$1"
  local jar_path
  jar_path="$(java_jar_path)"
  if [[ "$jar_path" == "unsupported" ]]; then
    echo "Unsupported platform for Java Maven install: $OS_NAME/$(uname -m)"
    exit 6
  fi
  mkdir -p "$maven_repo_local"
  mvn -q org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file \
    -Dmaven.repo.local="$maven_repo_local" \
    -Dfile="$jar_path" \
    -DgroupId=com.bifrore \
    -DartifactId=bifrore-java \
    -Dversion="$BIFRORE_VERSION" \
    -Dpackaging=jar \
    -DgeneratePom=true
}

run_java_jmh() {
  local maven_repo_local="$ROOT_DIR/.m2-bench"
  local classpath_file="$BUILD_DIR/java-bench-classpath.txt"
  local test_classes_dir="$ROOT_DIR/bindings/jni/test/target/test-classes"
  install_java_jar_to_local_maven "$maven_repo_local"
  (cd "$ROOT_DIR" && mvn -q -Dmaven.repo.local="$maven_repo_local" -Dbifrore.java.version="$BIFRORE_VERSION" -f bindings/jni/test/pom.xml -DskipTests test-compile)
  (cd "$ROOT_DIR" && mvn -q -Dmaven.repo.local="$maven_repo_local" -Dbifrore.java.version="$BIFRORE_VERSION" -f bindings/jni/test/pom.xml \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    -Dmdep.outputFile="$classpath_file" \
    -DincludeScope=test)
  java -cp "$test_classes_dir:$(cat "$classpath_file")" org.openjdk.jmh.Main "$@"
}

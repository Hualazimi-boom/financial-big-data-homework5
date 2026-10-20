#!/usr/bin/env bash
set -euo pipefail
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
cd "$(dirname "$0")/.."
: "${HADOOP_HOME:?Set HADOOP_HOME to your Hadoop installation}"
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT
mkdir -p target
javac -encoding UTF-8 -source 8 -target 8 -cp "$("$HADOOP_HOME/bin/hadoop" classpath --glob)" -d "$BUILD_DIR" src/main/java/edu/homework/IndexSetRelations.java
jar cfe target/financial-big-data-homework5-1.0.0.jar edu.homework.IndexSetRelations -C "$BUILD_DIR" .

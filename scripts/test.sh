#!/usr/bin/env bash
set -euo pipefail
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
cd "$(dirname "$0")/.."
PROJECT=$(pwd)
: "${HADOOP_HOME:?Set HADOOP_HOME}"
RESULT=$(mktemp -d)
# Keep the test results in a fresh temporary directory; never remove user output.
"$HADOOP_HOME/bin/hadoop" jar "$PROJECT/target/financial-big-data-homework5-1.0.0.jar" -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// "$PROJECT/tests/A.txt" "$PROJECT/tests/B.txt" "$RESULT/task1" "$RESULT/task2"
python3 scripts/verify.py tests/A.txt tests/B.txt "$RESULT/task1/part-r-00000" "$RESULT/task2/part-r-00000"
if "$HADOOP_HOME/bin/hadoop" jar "$PROJECT/target/financial-big-data-homework5-1.0.0.jar" -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// "$PROJECT/tests/A.txt" "$PROJECT/tests/B.txt" "$RESULT/task1" "$RESULT/task2" > "$RESULT/existing.log" 2>&1; then
  echo 'FAIL: existing output was accepted'; exit 1
fi
grep -q 'Output directory already exists' "$RESULT/existing.log"
echo 'PASS: existing outputs rejected without deletion'
echo "Test results: $RESULT"

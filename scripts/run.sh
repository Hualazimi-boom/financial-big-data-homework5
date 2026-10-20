#!/usr/bin/env bash

# 1. 创建 HDFS 输入目录
# 2. 上传 A.txt 和 B.txt
# 3. 提交任务1
# 4. 等待任务1成功
# 5. 提交任务2
# 6. 等待任务2成功
# 7. 从 HDFS 导出两份结果到项目目录
# 8. 检查两个 HDFS 输出中都有 _SUCCESS

set -euo pipefail
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
cd "$(dirname "$0")/.."
PROJECT_DIR=$(pwd)
: "${HADOOP_HOME:?Set HADOOP_HOME to your Hadoop installation}"
HDFS_BASE=${1:-/user/$(whoami)/homework5-$(date +%Y%m%d-%H%M%S)}
HADOOP="$HADOOP_HOME/bin/hadoop"
HDFS="$HADOOP_HOME/bin/hdfs"

"$HDFS" dfs -mkdir -p "$HDFS_BASE/input"
"$HDFS" dfs -put "$PROJECT_DIR/input/A.txt" "$PROJECT_DIR/input/B.txt" "$HDFS_BASE/input/"
"$HADOOP" jar "$PROJECT_DIR/target/financial-big-data-homework5-1.0.0.jar" "$HDFS_BASE/input/A.txt" "$HDFS_BASE/input/B.txt" "$HDFS_BASE/output_task1" "$HDFS_BASE/output_task2"
mkdir -p output_task1 output_task2
"$HDFS" dfs -cat "$HDFS_BASE/output_task1/part-r-00000" > output_task1/part-r-00000
"$HDFS" dfs -cat "$HDFS_BASE/output_task2/part-r-00000" > output_task2/part-r-00000
"$HDFS" dfs -test -e "$HDFS_BASE/output_task1/_SUCCESS"
"$HDFS" dfs -test -e "$HDFS_BASE/output_task2/_SUCCESS"
touch output_task1/_SUCCESS output_task2/_SUCCESS
echo "HDFS_BASE=$HDFS_BASE"

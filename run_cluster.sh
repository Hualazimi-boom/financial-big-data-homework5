#!/usr/bin/env bash
set -euo pipefail
# Usage: bash run_cluster.sh /lab2/train.csv /lab2/test.csv /lab2/result_run1
# Inputs and output are HDFS paths. Output must not already exist.
TRAIN=${1:?training CSV HDFS path required}
TEST=${2:?test CSV HDFS path required}
OUT=${3:?new output HDFS path required}
hadoop jar target/credit-mapreduce.jar edu.lab.CreditLab all "$TRAIN" "$TEST" "$OUT" \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.map.memory.mb=1024 \
  -Dmapreduce.reduce.memory.mb=1024 \
  -Dmapreduce.map.java.opts=-Xmx768m \
  -Dmapreduce.reduce.java.opts=-Xmx768m \
  -Dmapreduce.input.fileinputformat.split.maxsize=8388608
# Download all output parts; do not take only part-m-00000 for map-only predictions.
hdfs dfs -get "$OUT" "output-cluster-$(date +%Y%m%d-%H%M%S)"

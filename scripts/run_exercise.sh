#!/usr/bin/env bash
# Run a single exercise on Databricks
#
# Usage:
#   ./scripts/run_exercise.sh 01    # runs ex_01_split_variable_delimiter
#   ./scripts/run_exercise.sh 29    # runs ex_29_exam_assessment_report

set -euo pipefail

EXERCISE_NUM="${1:?Usage: $0 <exercise_number>}"

TASK_KEY=$(grep -o "ex_${EXERCISE_NUM}_[a-z_]*" databricks.yml | head -1)

if [ -z "$TASK_KEY" ]; then
    echo "Error: No exercise found for number '$EXERCISE_NUM'"
    echo "Available exercises:"
    grep -o "ex_[0-9]*_[a-z_]*" databricks.yml | sort -u
    exit 1
fi

echo "==> Running task: $TASK_KEY"
databricks bundle run spark_exercises --params task_key="$TASK_KEY"

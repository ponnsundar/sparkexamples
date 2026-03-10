#!/usr/bin/env bash
# Deploy Spark project to Databricks using Asset Bundles (DABs)
#
# Usage:
#   ./scripts/deploy.sh
#
# Prerequisites:
#   - Databricks CLI v0.200+ installed
#   - DATABRICKS_HOST and DATABRICKS_TOKEN env vars set
#     OR run: databricks configure --token

set -euo pipefail

echo "==> Validating bundle..."
databricks bundle validate

echo "==> Deploying..."
databricks bundle deploy

echo "==> Done. Run jobs with:"
echo "  databricks bundle run spark_exercises"
echo "  databricks bundle run example_pipeline"

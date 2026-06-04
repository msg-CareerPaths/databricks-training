#!/usr/bin/env bash
# Create the Unity Catalog catalog/schema/Volume and upload the local landing tree.
# Requires an authenticated Databricks CLI (`databricks auth login`).
# Usage: scripts/upload_to_volume.sh [CATALOG] [OUT_DIR]
set -euo pipefail

CATALOG="${1:-insurance}"
OUT="${2:-data/landing}"
VOLUME_URI="dbfs:/Volumes/${CATALOG}/landing/raw"

echo ">> Ensuring catalog/schema/volume exist in ${CATALOG}"
databricks catalogs create "${CATALOG}"            2>/dev/null || true
databricks schemas  create landing "${CATALOG}"    2>/dev/null || true
databricks volumes  create "${CATALOG}" landing raw MANAGED 2>/dev/null || true

echo ">> Uploading ${OUT} -> ${VOLUME_URI}"
databricks fs cp -r --overwrite "${OUT}" "${VOLUME_URI}"

echo ">> Contents:"
databricks fs ls "${VOLUME_URI}"

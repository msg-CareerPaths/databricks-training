#!/usr/bin/env bash
# Generate the insurance dataset locally (initial load + optional deltas).
# Usage: scripts/run_local_generate.sh [TARGET_MB] [OUT_DIR]
set -euo pipefail

TARGET_MB="${1:-500}"
OUT="${2:-data/landing}"

echo ">> Initial load (~${TARGET_MB} MB) -> ${OUT}"
python -m data_generator.generate --mode initial --target-mb "${TARGET_MB}" --out "${OUT}"

# Deltas (~50 MB each). Batch 2 introduces the telematics.device_fw schema-drift column.
echo ">> Delta batch 1"
python -m data_generator.generate --mode delta --batch 1 --out "${OUT}"
echo ">> Delta batch 2 (schema drift)"
python -m data_generator.generate --mode delta --batch 2 --out "${OUT}"

echo ">> Done. Landing tree:"
du -sh "${OUT}"/*/ 2>/dev/null || true

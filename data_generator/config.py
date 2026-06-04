"""Configuration knobs for the insurance synthetic-data generator.

Everything that controls *what* and *how much* gets generated lives here:
the master seed, per-source entity counts (scaled to a target size on disk),
file chunking, business rates, and the data-quality (DQ) defect injection rates.

The whole generator is deterministic given ``MASTER_SEED`` + the CLI args, so two
runs with the same arguments produce byte-identical output. That property is what
the ``tests/`` suite and the studybook's "idempotency" lessons rely on.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# --------------------------------------------------------------------------- #
# Reproducibility
# --------------------------------------------------------------------------- #
MASTER_SEED = 20260504  # the exam version date, 2026-05-04 — easy to remember

# --------------------------------------------------------------------------- #
# Output layout
# --------------------------------------------------------------------------- #
DEFAULT_OUT = "data/landing"
# Folder names under the landing zone (one per source). These become the
# per-source directories Auto Loader / COPY INTO read from in the bronze layer.
SOURCE_DIRS = (
    "customers",
    "policies",
    "claims",
    "payments",
    "agents",
    "telematics",
    "reference",
)

# Committed lookup CSVs that ship with the repo (copied into landing/reference).
REFERENCE_DIR = Path(__file__).resolve().parent / "reference"
REFERENCE_FILES = (
    "us_states.csv",
    "vehicle_makes.csv",
    "peril_codes.csv",
    "coverage_types.csv",
    "claim_status_ref.csv",
    "postal_region.csv",
)

# Manifest written on an initial run; deltas read it to reference existing ids.
MANIFEST_NAME = "_manifest.json"

# --------------------------------------------------------------------------- #
# Sizing
# --------------------------------------------------------------------------- #
# Entity counts are tuned so an *initial* run at BASE_TARGET_MB lands ~that many
# MB on disk, with telematics JSONL acting as the "filler" that tops the landing
# zone up to the requested target. Smaller targets scale the counts down linearly
# so a `--target-mb 20` smoke run stays fast.
BASE_TARGET_MB = 500
TARGET_MB_DEFAULT = 500

BASE_COUNTS = {
    "customers": 250_000,
    "agents": 3_000,
    "policies": 300_000,
    "claims": 180_000,
    "payments": 2_500_000,
    # telematics is not a fixed count — generate.py tops it up to the target.
}

# Rows per output file → keeps "many files per source" so Auto Loader has
# discrete files to discover and COPY INTO has multiple inputs.
ROWS_PER_FILE = {
    "customers": 25_000,
    "agents": 1_500,
    "policies": 20_000,
    "claims": 20_000,
    "payments": 250_000,
    "telematics": 150_000,
}

# Telematics events generated per AUTO policy on the initial load (before top-up).
TELEMATICS_EVENTS_PER_POLICY = 6

# Deltas (~50 MB each): a trickle of new business + entity *updates* (to exercise
# MERGE / SCD2 in silver) + a telematics fill.
DELTA_TARGET_MB = 50
DELTA_COUNTS = {
    "customers_updates": 8_000,  # changed address / phone / status -> SCD2 source
    "agents_updates": 150,       # changed branch / status -> SCD2 source
    "policies": 6_000,
    "claims": 4_000,
    "payments": 60_000,
}
# Delta batch index (``--batch``) that introduces the schema-drift column
# (``telematics.device_fw``) — used to teach Auto Loader schema evolution.
SCHEMA_DRIFT_BATCH = 2

# --------------------------------------------------------------------------- #
# Business knobs
# --------------------------------------------------------------------------- #
AUTO_SHARE = 0.60            # fraction of policies that are AUTO (rest PROPERTY)
COMMERCIAL_SHARE = 0.18      # fraction of customers that are COMMERCIAL segment
CLAIM_POLICY_RATE = 0.35     # fraction of policies that have >= 1 claim
FRAUD_RATE = 0.04            # fraction of claims flagged as fraud
LATE_PAYMENT_RATE = 0.12     # fraction of installments paid late
MISSED_PAYMENT_RATE = 0.04   # fraction of installments missed (no paid_date)


# --------------------------------------------------------------------------- #
# Data-quality defect injection rates (fraction of rows affected, per defect)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DQRates:
    """Each rate is the probability a given row is hit by that defect.

    Every defect here maps to a documented cleaning task in the silver layer, so
    participants must actually clean the data to pass the milestone acceptance
    criteria. See ``docs/03_data_dictionary.md`` for the defect catalogue.
    """

    null_required: float = 0.02       # nulls in required fields
    exact_duplicate: float = 0.010    # fully duplicated rows
    fuzzy_duplicate: float = 0.008    # near-duplicate rows (same entity, minor diffs)
    casing_whitespace: float = 0.05   # inconsistent casing / stray whitespace
    invalid_category: float = 0.03    # typos / invalid categorical values
    out_of_range: float = 0.015       # negative premium, loss > sum_insured, etc.
    bad_date: float = 0.02            # multi-format / future / malformed dates
    orphan_fk: float = 0.010          # foreign key with no matching parent
    numeric_as_string: float = 0.02   # numbers stored as strings with junk chars
    late_event: float = 0.03          # telematics late / out-of-order timestamps


DQ = DQRates()


def scaled_counts(target_mb: int) -> dict[str, int]:
    """Scale the base entity counts to ``target_mb`` (linear vs BASE_TARGET_MB).

    A small floor keeps tiny smoke-test targets from producing empty sources.
    Telematics is intentionally excluded — generate.py fills it to the target.
    """
    scale = max(target_mb / BASE_TARGET_MB, 0.0)
    floors = {"agents": 25}
    out: dict[str, int] = {}
    for name, base in BASE_COUNTS.items():
        out[name] = max(int(round(base * scale)), floors.get(name, 100))
    return out

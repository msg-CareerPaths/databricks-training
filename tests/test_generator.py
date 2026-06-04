"""Sanity + data-quality tests for the insurance generator.

These run fast (small/scaled generations) and assert the three properties the
studybook leans on: determinism, multi-file/multi-format output, and the presence
of the seeded DQ defects that make the silver cleaning milestone meaningful.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from data_generator import common, config, generate
from data_generator.generators import customers as gen_customers
from data_generator.generators import telematics as gen_telematics


def _noop(*_args, **_kwargs):
    return None


@pytest.fixture
def ref():
    return generate._prepare_reference()


@pytest.fixture
def pools():
    return common.build_pools(common.make_faker(1), 2000, 2000, 1500, 1000)


# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
def test_reference_files_load():
    for fname in config.REFERENCE_FILES:
        df = common.load_reference(fname)
        assert len(df) > 0, f"{fname} is empty"
    states = common.load_reference("us_states.csv")
    assert len(states) == 51  # 50 states + DC


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def test_customers_deterministic(ref, pools):
    a = gen_customers.generate(800, common.make_rng(42), pools, ref, config.DQ)
    b = gen_customers.generate(800, common.make_rng(42), pools, ref, config.DQ)
    pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))


def test_customers_changes_with_seed(ref, pools):
    a = gen_customers.generate(800, common.make_rng(1), pools, ref, config.DQ)
    b = gen_customers.generate(800, common.make_rng(2), pools, ref, config.DQ)
    assert not a.equals(b)


# --------------------------------------------------------------------------- #
# Seeded data-quality defects must be present
# --------------------------------------------------------------------------- #
def test_customers_have_seeded_defects(ref, pools):
    df = gen_customers.generate(6000, common.make_rng(7), pools, ref, config.DQ)
    valid_states = set(common.load_reference("us_states.csv")["state_code"])
    # duplicates injected -> more rows than requested
    assert len(df) > 6000
    # some null emails
    assert df["email"].isna().sum() > 0
    # some invalid / typo state codes
    invalid = (~df["state"].isin(valid_states)).sum()
    assert invalid > 0
    # some malformed dates (non ISO YYYY-MM-DD)
    bad_dates = (~df["date_of_birth"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")).sum()
    assert bad_dates > 0


def test_telematics_schema_drift_flag(ref):
    pol = common.make_ids("POL", 1, 50, 9)
    cust = common.make_ids("CUST", 1, 50, 8)
    no_fw = gen_telematics.generate_events(500, common.make_rng(3), pol, cust, config.DQ, include_fw=False)
    fw = gen_telematics.generate_events(500, common.make_rng(3), pol, cust, config.DQ, include_fw=True)
    assert "device_fw" not in no_fw.columns
    assert "device_fw" in fw.columns


# --------------------------------------------------------------------------- #
# End-to-end smoke: small initial run touches every writer + format
# --------------------------------------------------------------------------- #
def test_initial_smoke_all_sources_and_formats(tmp_path):
    out = tmp_path / "landing"
    manifest = generate.generate_initial(str(out), target_mb=8, seed=config.MASTER_SEED, log=_noop)

    for d in ("customers", "policies", "claims", "payments", "agents", "telematics", "reference"):
        files = list((out / d).glob("*"))
        assert files, f"no files written for {d}"

    # manifest present and self-consistent
    assert (out / config.MANIFEST_NAME).exists()
    assert manifest["counts"]["customers"] > 0

    # every format is readable
    pd.read_csv(next((out / "customers").glob("*.csv")))
    with open(next((out / "policies").glob("*.json"))) as fh:
        policies = json.load(fh)
    assert isinstance(policies, list) and "coverages" in policies[0]
    pd.read_json(next((out / "claims").glob("*.json")), lines=True)
    pd.read_parquet(next((out / "payments").glob("*.parquet")))
    pd.read_json(next((out / "telematics").glob("*.jsonl")), lines=True)

    # telematics actually filled toward the target
    assert manifest["size_mb"] >= 5


def test_delta_schema_drift_adds_device_fw(tmp_path, monkeypatch):
    # shrink delta volumes so the test stays fast
    monkeypatch.setattr(config, "DELTA_TARGET_MB", 2)
    monkeypatch.setattr(config, "DELTA_COUNTS", {
        "customers_updates": 300, "agents_updates": 20, "policies": 200, "claims": 150, "payments": 1500,
    })
    out = tmp_path / "landing"
    generate.generate_initial(str(out), target_mb=6, seed=config.MASTER_SEED, log=_noop)
    result = generate.generate_delta(str(out), batch=config.SCHEMA_DRIFT_BATCH, seed=config.MASTER_SEED, log=_noop)

    assert result["schema_drift"] is True
    drift_files = list((out / "telematics").glob(f"telematics_delta{config.SCHEMA_DRIFT_BATCH}_*.jsonl"))
    assert drift_files
    sample = pd.read_json(drift_files[0], lines=True)
    assert "device_fw" in sample.columns
    # update files for SCD2 exist
    assert list((out / "customers").glob(f"customers_delta{config.SCHEMA_DRIFT_BATCH}_*.csv"))

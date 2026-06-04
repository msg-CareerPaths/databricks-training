"""Generate the ``agents`` source — CSV reference dimension; SCD2 target via deltas.

Kept relatively clean (it is a dimension); deltas mutate branch/status/commission
with a newer ``updated_at`` so silver can build a Type-2 history with MERGE.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common


def generate(n, rng, pools, ref, dq, id_start: int = 1) -> pd.DataFrame:
    ids = common.make_ids("AGT", id_start, n, 5)
    fi = pools["first"][rng.integers(0, len(pools["first"]), n)].astype(str)
    li = pools["last"][rng.integers(0, len(pools["last"]), n)].astype(str)
    email = np.char.add(
        np.char.add(np.char.add(np.char.lower(fi), "."), np.char.lower(li)),
        "@agency.example.com",
    )
    state = ref["state_codes"][rng.integers(0, len(ref["state_codes"]), n)]
    region = pd.Series(state).map(ref["state_region"]).fillna("South").to_numpy()
    city = pools["cities"][rng.integers(0, len(pools["cities"]), n)].astype(str)
    branch = np.char.add(city, " Branch")
    hire = common.iso_date(common.random_dates(rng, "2005-01-01", "2025-12-31", n))
    status = np.where(rng.random(n) < 0.85, "ACTIVE", "INACTIVE")
    commission = np.round(rng.uniform(0.05, 0.20, n), 3)
    updated = common.iso_ts(common.random_datetimes(rng, "2024-01-01", "2026-05-01", n))

    df = pd.DataFrame(
        {
            "agent_id": ids,
            "first_name": fi,
            "last_name": li,
            "email": email,
            "branch": branch,
            "region": region,
            "state": state,
            "hire_date": hire,
            "status": status,
            "commission_rate": commission,
            "updated_at": updated,
        }
    )
    df = common.inject_nulls(df, ["email"], rng, dq.null_required)
    df["branch"] = common.inject_casing_whitespace(df["branch"], rng, dq.casing_whitespace)
    return df

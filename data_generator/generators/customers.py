"""Generate the ``customers`` source — CSV (CRM export), PII, SCD source via deltas.

Seeded defects: nulls (email/phone), casing/whitespace (city), invalid/typo state
codes, malformed/future birth dates, plus exact & fuzzy duplicate rows.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common, config

REGION_ZIP1 = {"Northeast": "0", "South": "3", "Midwest": "5", "West": "9"}
INVALID_STATES = np.array(
    ["ca", "tx", "Ny", " FL", "XX", "ZZ", "Calif", "N/A", "99", "us"], dtype=object
)
EMAIL_DOMAINS = np.array(["example.com", "mail.com", "test.org", "acme.net"])
GENDERS = np.array(["M", "F", "X"])


def generate(n, rng, pools, ref, dq, id_start: int = 1) -> pd.DataFrame:
    ids = common.make_ids("CUST", id_start, n, 8)
    fi = pools["first"][rng.integers(0, len(pools["first"]), n)].astype(str)
    li = pools["last"][rng.integers(0, len(pools["last"]), n)].astype(str)

    local = np.char.add(np.char.add(np.char.lower(fi), "."), np.char.lower(li))
    local = np.char.add(local, (np.arange(id_start, id_start + n) % 97).astype(str))
    email = np.char.add(np.char.add(local, "@"), EMAIL_DOMAINS[rng.integers(0, len(EMAIL_DOMAINS), n)])

    area = rng.integers(200, 1000, n)
    mid = rng.integers(200, 1000, n)
    ln = rng.integers(0, 10000, n)
    phone = [f"({area[i]}) {mid[i]:03d}-{ln[i]:04d}" for i in range(n)]

    state = ref["state_codes"][rng.integers(0, len(ref["state_codes"]), n)]
    region = pd.Series(state).map(ref["state_region"]).fillna("South").to_numpy()
    zip1 = np.array([REGION_ZIP1[r] for r in region])
    postal = np.char.add(zip1, np.char.zfill(rng.integers(0, 10000, n).astype(str), 4))

    city = pools["cities"][rng.integers(0, len(pools["cities"]), n)].astype(str)
    street = pools["streets"][rng.integers(0, len(pools["streets"]), n)].astype(str)

    dob = common.iso_date(common.random_dates(rng, "1945-01-01", "2005-12-31", n))
    since = common.iso_date(common.random_dates(rng, "2008-01-01", "2026-05-01", n))
    created = common.iso_ts(common.random_datetimes(rng, "2008-01-01", "2026-05-01", n))
    segment = np.where(rng.random(n) < config.COMMERCIAL_SHARE, "COMMERCIAL", "PERSONAL")
    gender = GENDERS[rng.integers(0, 3, n)]

    df = pd.DataFrame(
        {
            "customer_id": ids,
            "first_name": fi,
            "last_name": li,
            "email": email,
            "phone": phone,
            "address_line1": street,
            "city": city,
            "state": state,
            "postal_code": postal,
            "date_of_birth": dob,
            "gender": gender,
            "customer_since": since,
            "segment": segment,
            "created_at": created,
            "updated_at": created,
        }
    )

    # ---- seeded data-quality defects ----
    df = common.inject_nulls(df, ["email", "phone"], rng, dq.null_required)
    df["city"] = common.inject_casing_whitespace(df["city"], rng, dq.casing_whitespace)
    df["state"] = common.inject_invalid_category(df["state"], rng, dq.invalid_category, INVALID_STATES)
    df["date_of_birth"] = common.corrupt_dates(df["date_of_birth"], rng, dq.bad_date)
    df = common.append_exact_duplicates(df, rng, dq.exact_duplicate)
    df = common.append_fuzzy_duplicates(df, rng, dq.fuzzy_duplicate, ["first_name", "last_name"])
    df = common.shuffle(df, rng)
    return df

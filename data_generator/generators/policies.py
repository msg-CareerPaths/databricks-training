"""Generate the ``policies`` source — nested JSON with a ``coverages[]`` array.

Returns BOTH the JSON records (written as multi-line JSON arrays, read with
``multiLine=true``) and a clean ``summary`` DataFrame the downstream claims /
payments / telematics generators use for referential integrity.

Seeded defects (in the JSON only; summary stays clean): status typos, negative &
string premiums (mixed types), orphan customer_id, malformed/future effective dates.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common, config

STATUS_VALUES = np.array(["ACTIVE", "CANCELLED", "LAPSED", "PENDING"])
STATUS_WEIGHTS = np.array([0.72, 0.12, 0.10, 0.06])
STATUS_TYPOS = np.array(
    ["active", "Active ", "ACTIVEE", "cancelled", "LAPSE", "pend", "Pening", "Actv"], dtype=object
)
FREQS = np.array(["ANNUAL", "SEMI_ANNUAL", "QUARTERLY", "MONTHLY"])
FREQ_WEIGHTS = np.array([0.25, 0.20, 0.20, 0.35])
CONSTRUCTION = np.array(["Frame", "Masonry", "Brick", "Concrete"])


def generate(n, rng, customer_ids, agent_ids, ref, dq, id_start: int = 1):
    pid = common.make_ids("POL", id_start, n, 9)
    cust = customer_ids[rng.integers(0, len(customer_ids), n)].astype(object)
    agent = agent_ids[rng.integers(0, len(agent_ids), n)].astype(str)
    is_auto = rng.random(n) < config.AUTO_SHARE
    product_line = np.where(is_auto, "AUTO", "PROPERTY")

    status = rng.choice(STATUS_VALUES, size=n, p=STATUS_WEIGHTS)
    freq = rng.choice(FREQS, size=n, p=FREQ_WEIGHTS)

    eff = common.random_dates(rng, "2018-01-01", "2026-04-01", n)  # datetime64[D]
    exp = eff + np.timedelta64(365, "D")
    eff_str = common.iso_date(eff)
    exp_str = common.iso_date(exp)

    premium = np.where(is_auto, rng.normal(1200, 350, n), rng.normal(1600, 600, n)).clip(min=150).round(2)
    sum_insured = np.where(is_auto, rng.uniform(5_000, 60_000, n), rng.uniform(150_000, 800_000, n)).round(0)

    makes = ref["makes"]
    midx = rng.integers(0, len(makes), n)
    veh_make = makes["make"].to_numpy()[midx]
    veh_model = makes["model"].to_numpy()[midx]
    veh_type = makes["vehicle_type"].to_numpy()[midx]
    veh_year = rng.integers(2005, 2026, n)
    vin = common.make_ids("1HGCM", id_start, n, 11)
    construction = CONSTRUCTION[rng.integers(0, len(CONSTRUCTION), n)]
    year_built = rng.integers(1950, 2021, n)
    sq_ft = rng.integers(800, 4500, n)

    # coverages: precompute random matrices, slice ncov per row in the assembly loop
    ncov = rng.integers(1, 5, n)
    cov_r = rng.integers(0, 1000, (n, 4))
    per_r = rng.integers(0, 1000, (n, 4))
    limits = rng.integers(10, 1001, (n, 4)) * 1000
    deds = rng.choice(np.array([250, 500, 1000, 2500, 5000]), size=(n, 4))
    auto_cov, prop_cov = ref["coverages_auto"], ref["coverages_property"]
    auto_per, prop_per = ref["perils_auto"], ref["perils_property"]

    # ---- DQ-corrupted copies for the JSON (summary keeps clean values) ----
    cust_out = cust.copy()
    orphan_m = common.dq_mask(rng, n, dq.orphan_fk)
    if orphan_m.any():
        cust_out[orphan_m] = common.make_ids("CUST", 90_000_000, int(orphan_m.sum()), 8)
    status_out = common.inject_invalid_category(pd.Series(status), rng, dq.invalid_category, STATUS_TYPOS).to_numpy()
    eff_out = common.corrupt_dates(pd.Series(eff_str), rng, dq.bad_date).to_numpy()
    premium_out = premium.astype(object).copy()
    neg_m = common.dq_mask(rng, n, dq.out_of_range)
    premium_out[neg_m] = (-premium[neg_m]).round(2)
    premium_out = common.inject_numeric_as_string(pd.Series(premium_out), rng, dq.numeric_as_string).to_numpy()

    records = []
    for i in range(n):
        if is_auto[i]:
            cpool, ppool = auto_cov, auto_per
        else:
            cpool, ppool = prop_cov, prop_per
        seen, covs = set(), []
        for j in range(int(ncov[i])):
            code = cpool[cov_r[i, j] % len(cpool)]
            if code in seen:
                continue
            seen.add(code)
            covs.append(
                {
                    "coverage_code": code,
                    "limit": int(limits[i, j]),
                    "deductible": int(deds[i, j]),
                    "peril_code": ppool[per_r[i, j] % len(ppool)],
                }
            )
        rec = {
            "policy_id": pid[i],
            "customer_id": cust_out[i],
            "agent_id": agent[i],
            "product_line": product_line[i],
            "status": status_out[i],
            "effective_date": eff_out[i],
            "expiration_date": exp_str[i],
            "annual_premium": premium_out[i],
            "sum_insured": float(sum_insured[i]),
            "payment_frequency": freq[i],
            "coverages": covs,
        }
        if is_auto[i]:
            rec["vehicle"] = {
                "make": veh_make[i], "model": veh_model[i], "year": int(veh_year[i]),
                "vin": vin[i], "vehicle_type": veh_type[i],
            }
        else:
            rec["property"] = {
                "construction": construction[i], "year_built": int(year_built[i]), "sq_ft": int(sq_ft[i]),
            }
        records.append(rec)

    summary = pd.DataFrame(
        {
            "policy_id": pid,
            "customer_id": cust.astype(str),
            "agent_id": agent,
            "product_line": product_line,
            "annual_premium": premium,
            "sum_insured": sum_insured,
            "effective_date": eff,
            "expiration_date": exp,
            "payment_frequency": freq,
            "is_auto": is_auto,
        }
    )
    return records, summary

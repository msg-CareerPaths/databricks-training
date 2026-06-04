"""Generate the ``claims`` source — newline-delimited JSON.

Seeded defects: status typos, malformed/future claim dates, nulls (peril/amount),
orphan policy_id, ``loss_amount`` exceeding ``sum_insured`` (out-of-range), and a
mixed-representation ``fraud_flag`` (true/Y/1/YES vs false/N/0/NO) to clean.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common, config

STATUS = np.array(["OPEN", "CLOSED", "DENIED", "IN_REVIEW"])
STATUS_W = np.array([0.30, 0.45, 0.12, 0.13])
STATUS_TYPOS = np.array(
    ["open", "Closed ", "denied", "in review", "CLOSE", "opn", "Reviewing"], dtype=object
)
DESCRIPTIONS = np.array(
    [
        "Rear-end collision at intersection", "Hail damage to roof", "Water damage from burst pipe",
        "Theft of vehicle", "Windshield cracked by debris", "Kitchen fire", "Parking lot fender bender",
        "Tree fell on garage", "Flood damage to basement", "Vandalism to property",
    ]
)
FRAUD_TRUE = np.array(["true", "Y", "1", "YES"], dtype=object)
FRAUD_FALSE = np.array(["false", "N", "0", "NO"], dtype=object)


def generate(n, rng, policy_summary, ref, dq, id_start: int = 1) -> pd.DataFrame:
    P = len(policy_summary)
    pol = policy_summary.iloc[rng.integers(0, P, n)].reset_index(drop=True)
    is_auto = pol["is_auto"].to_numpy()
    sum_insured = pol["sum_insured"].to_numpy().astype(float)

    cid = common.make_ids("CLM", id_start, n, 9)
    loss = common.random_dates(rng, "2020-01-01", "2026-04-15", n)
    claim = loss + rng.integers(0, 20, n).astype("timedelta64[D]")

    auto_per = np.array(ref["perils_auto"])
    prop_per = np.array(ref["perils_property"])
    peril = np.where(
        is_auto,
        auto_per[rng.integers(0, len(auto_per), n)],
        prop_per[rng.integers(0, len(prop_per), n)],
    )
    status = rng.choice(STATUS, size=n, p=STATUS_W)

    reported = (sum_insured * rng.uniform(0.05, 0.8, n)).round(2)
    loss_amount = reported.copy()
    over_m = common.dq_mask(rng, n, dq.out_of_range)
    if over_m.any():
        loss_amount[over_m] = (sum_insured[over_m] * rng.uniform(1.1, 1.6, int(over_m.sum()))).round(2)
    paid = np.where(status == "CLOSED", (loss_amount * rng.uniform(0.6, 1.0, n)).round(2), 0.0)

    fraud_bool = rng.random(n) < config.FRAUD_RATE
    fraud_flag = np.where(fraud_bool, FRAUD_TRUE[rng.integers(0, 4, n)], FRAUD_FALSE[rng.integers(0, 4, n)])

    pid_out = pol["policy_id"].to_numpy().astype(object)
    orphan_m = common.dq_mask(rng, n, dq.orphan_fk)
    if orphan_m.any():
        pid_out[orphan_m] = common.make_ids("POL", 900_000_000, int(orphan_m.sum()), 9)

    df = pd.DataFrame(
        {
            "claim_id": cid,
            "policy_id": pid_out,
            "customer_id": pol["customer_id"].to_numpy(),
            "loss_date": common.iso_date(loss),
            "claim_date": common.iso_date(claim),
            "peril_code": peril,
            "claim_status": status,
            "reported_amount": reported,
            "loss_amount": loss_amount,
            "paid_amount": paid,
            "fraud_flag": fraud_flag,
            "description": DESCRIPTIONS[rng.integers(0, len(DESCRIPTIONS), n)],
        }
    )
    df["claim_status"] = common.inject_invalid_category(df["claim_status"], rng, dq.invalid_category, STATUS_TYPOS)
    df["claim_date"] = common.corrupt_dates(df["claim_date"], rng, dq.bad_date)
    df = common.inject_nulls(df, ["peril_code", "reported_amount"], rng, dq.null_required)
    df = common.append_exact_duplicates(df, rng, dq.exact_duplicate)
    df = common.shuffle(df, rng)
    return df

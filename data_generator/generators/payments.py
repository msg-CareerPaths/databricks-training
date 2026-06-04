"""Generate the ``payments`` source — Parquet (typed finance export), via COPY INTO.

Installments are derived per policy from its payment frequency. The data is typed
(Parquet) but *logically* dirty: missed installments have a null ``paid_date``,
late ones have ``paid_date > due_date``, some ``amount_due`` are negative, and
future installments are PENDING — all of which silver must reconcile.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common, config

FREQ_INSTALLMENTS = {"ANNUAL": 1, "SEMI_ANNUAL": 2, "QUARTERLY": 4, "MONTHLY": 12}
METHODS = np.array(["ACH", "CARD", "CHECK", "CASH"])
TODAY = np.datetime64("2026-05-04", "D")


def generate(rng, policy_summary, dq, id_start: int = 1) -> pd.DataFrame:
    inst = policy_summary["payment_frequency"].map(FREQ_INSTALLMENTS).fillna(1).astype(int).to_numpy()
    P = len(policy_summary)
    total = int(inst.sum())

    pol_idx = np.repeat(np.arange(P), inst)
    starts = np.cumsum(inst) - inst
    inst_no = np.arange(total) - np.repeat(starts, inst) + 1

    premium = policy_summary["annual_premium"].to_numpy()[pol_idx]
    counts = inst[pol_idx]
    amount_due = (premium / counts).round(2)
    period_days = np.maximum((365.0 / counts).astype(int), 1)
    eff_days = policy_summary["effective_date"].to_numpy().astype("datetime64[D]")[pol_idx]
    due = eff_days + ((inst_no - 1) * period_days).astype("timedelta64[D]")

    r = rng.random(total)
    missed = r < config.MISSED_PAYMENT_RATE
    late = (~missed) & (r < config.MISSED_PAYMENT_RATE + config.LATE_PAYMENT_RATE)
    pending = due > TODAY

    paid_off = np.where(late, rng.integers(5, 45, total), rng.integers(-3, 3, total)).astype("timedelta64[D]")
    paid_date = (due + paid_off).astype("datetime64[D]")
    paid_str = common.iso_date(paid_date).astype(object)
    paid_str[missed] = None
    paid_str[pending] = None

    status = np.where(missed, "MISSED", np.where(late, "LATE", "PAID"))
    status = np.where(pending & ~missed, "PENDING", status)

    amount_paid = np.where(late | (status == "PAID"), amount_due, 0.0).astype(float)
    amount_paid[missed] = np.nan
    amount_paid[pending] = np.nan

    df = pd.DataFrame(
        {
            "payment_id": common.make_ids("PAY", id_start, total, 10),
            "policy_id": policy_summary["policy_id"].to_numpy()[pol_idx],
            "installment_no": inst_no.astype("int32"),
            "due_date": pd.to_datetime(common.iso_date(due)),
            "paid_date": pd.to_datetime(pd.Series(paid_str), errors="coerce"),
            "amount_due": amount_due,
            "amount_paid": amount_paid,
            "payment_method": METHODS[rng.integers(0, len(METHODS), total)],
            "status": status,
        }
    )
    neg_m = common.dq_mask(rng, total, dq.out_of_range)
    df.loc[neg_m, "amount_due"] = -df.loc[neg_m, "amount_due"]
    return df

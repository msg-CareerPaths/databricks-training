"""Generate the ``telematics`` source — JSONL per-trip events (bulk of the dataset).

Generated in chunks by ``generate.py`` to fill the landing zone up to the target
size. Seeded defects: late / out-of-order timestamps (watermarking lesson),
out-of-range speeds (negative or >300), and nulls on lat/lon/speed. The optional
``device_fw`` column is added only in the schema-drift delta batch to exercise
Auto Loader schema evolution.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import common

# Rough contiguous-US lat/lon bounding box.
LAT_MIN, LAT_MAX = 25.0, 49.0
LON_MIN, LON_MAX = -124.0, -67.0
FW_VERSIONS = np.array(["fw_2.0.1", "fw_2.1.0", "fw_2.1.3", "fw_3.0.0"])


def generate_events(n, rng, auto_policy_ids, auto_customer_ids, dq, id_start: int = 1,
                    include_fw: bool = False) -> pd.DataFrame:
    pidx = rng.integers(0, len(auto_policy_ids), n)
    eid = common.make_ids("EVT", id_start, n, 12)
    trip_id = common.make_ids("TRP", id_start // 25 + 1, n, 12)

    ts = common.random_datetimes(rng, "2024-01-01", "2026-05-04", n).astype("datetime64[s]")
    late_m = common.dq_mask(rng, n, dq.late_event)
    if late_m.any():
        shift = rng.integers(3600, 7 * 24 * 3600, int(late_m.sum())).astype("timedelta64[s]")
        ts[late_m] = ts[late_m] - shift

    speed = rng.uniform(0, 130, n).round(1).astype(object)
    bad_m = common.dq_mask(rng, n, dq.out_of_range)
    if bad_m.any():
        k = int(bad_m.sum())
        bad_vals = np.where(rng.random(k) < 0.5, -rng.uniform(1, 20, k), rng.uniform(300, 500, k)).round(1)
        speed[bad_m] = bad_vals

    data = {
        "event_id": eid,
        "trip_id": trip_id,
        "policy_id": auto_policy_ids[pidx],
        "customer_id": auto_customer_ids[pidx],
        "event_ts": common.iso_ts(ts),
        "latitude": rng.uniform(LAT_MIN, LAT_MAX, n).round(5),
        "longitude": rng.uniform(LON_MIN, LON_MAX, n).round(5),
        "speed_kmh": speed,
        "heading": rng.integers(0, 360, n),
        "harsh_brake": rng.random(n) < 0.05,
        "harsh_accel": rng.random(n) < 0.05,
        "harsh_corner": rng.random(n) < 0.03,
        "mileage": rng.uniform(0, 400, n).round(1),
        "device_id": common.make_ids("DEV", 1, n, 6),
    }
    if include_fw:
        data["device_fw"] = FW_VERSIONS[rng.integers(0, len(FW_VERSIONS), n)]

    df = pd.DataFrame(data)
    df = common.inject_nulls(df, ["latitude", "longitude", "speed_kmh"], rng, dq.null_required)
    return df

"""CLI entry point for the insurance synthetic-data generator.

Examples
--------
Initial ~500 MB load:
    python -m data_generator.generate --mode initial --target-mb 500 --out data/landing

Fast smoke run:
    python -m data_generator.generate --mode initial --target-mb 20 --out data/_smoke

Delta batch (batch 2 introduces the telematics.device_fw schema-drift column):
    python -m data_generator.generate --mode delta --batch 2 --out data/landing

The landing zone mirrors the UC Volume layout (one folder per source), so the
same tree can be uploaded with ``databricks fs cp -r data/landing <volume>``.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from data_generator import common, config
from data_generator.generators import agents as gen_agents
from data_generator.generators import claims as gen_claims
from data_generator.generators import customers as gen_customers
from data_generator.generators import payments as gen_payments
from data_generator.generators import policies as gen_policies
from data_generator.generators import telematics as gen_telematics


def _prepare_reference() -> dict:
    states = common.load_reference("us_states.csv")
    perils = common.load_reference("peril_codes.csv")
    covs = common.load_reference("coverage_types.csv")
    makes = common.load_reference("vehicle_makes.csv")
    return {
        "state_codes": states["state_code"].to_numpy(),
        "state_region": dict(zip(states["state_code"], states["region"])),
        "makes": makes,
        "perils_auto": perils[perils["product_line"] == "AUTO"]["peril_code"].tolist(),
        "perils_property": perils[perils["product_line"] == "PROPERTY"]["peril_code"].tolist(),
        "coverages_auto": covs[covs["product_line"] == "AUTO"]["coverage_code"].tolist(),
        "coverages_property": covs[covs["product_line"] == "PROPERTY"]["coverage_code"].tolist(),
    }


def _fill_telematics(out_dir, target_bytes, rng, auto_pol, auto_cust, ev_start, file_start,
                     prefix, include_fw, baseline_bytes, log):
    """Write telematics JSONL chunks until the landing zone grows by the target.

    Chunk sizes adapt to the *measured* bytes-per-event so the result lands close
    to the target at any scale (a fixed chunk badly overshoots small targets).
    ``baseline_bytes`` lets deltas size only the *newly added* telematics.
    """
    tdir = Path(out_dir) / "telematics"
    tdir.mkdir(parents=True, exist_ok=True)
    cap = config.ROWS_PER_FILE["telematics"]
    min_chunk = 2_000
    bytes_per_event = 320.0  # initial estimate, refined after the first write
    ev, fidx = ev_start, file_start
    while True:
        remaining = target_bytes - (common.dir_size_bytes(out_dir) - baseline_bytes)
        if remaining <= 0:
            break
        n = max(min(int(remaining / bytes_per_event), cap), min_chunk)
        df = gen_telematics.generate_events(n, rng, auto_pol, auto_cust, config.DQ,
                                            id_start=ev, include_fw=include_fw)
        p = tdir / f"{prefix}_{fidx:04d}.jsonl"
        df.to_json(p, orient="records", lines=True, date_format="iso")
        bytes_per_event = max(p.stat().st_size / n, 50.0)  # refine estimate
        ev += n
        fidx += 1
        if fidx - file_start > 5000:
            log("  ! telematics safety cap reached")
            break
    return ev, fidx


def generate_initial(out_dir, target_mb, seed, log) -> dict:
    rng = common.make_rng(seed)
    pools = common.build_pools(common.make_faker(seed))
    ref = _prepare_reference()
    counts = config.scaled_counts(target_mb)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    common.copy_reference_to_landing(out)

    log(f"customers x{counts['customers']:,}")
    cust_df = gen_customers.generate(counts["customers"], rng, pools, ref, config.DQ)
    common.write_csv_chunks(cust_df, out / "customers", "customers", config.ROWS_PER_FILE["customers"])
    customer_ids = common.make_ids("CUST", 1, counts["customers"], 8)

    log(f"agents x{counts['agents']:,}")
    agents_df = gen_agents.generate(counts["agents"], rng, pools, ref, config.DQ)
    common.write_csv_chunks(agents_df, out / "agents", "agents", config.ROWS_PER_FILE["agents"])
    agent_ids = common.make_ids("AGT", 1, counts["agents"], 5)

    log(f"policies x{counts['policies']:,}")
    pol_records, pol_sum = gen_policies.generate(counts["policies"], rng, customer_ids, agent_ids, ref, config.DQ)
    common.write_json_array_chunks(pol_records, out / "policies", "policies", config.ROWS_PER_FILE["policies"])

    log(f"claims x{counts['claims']:,}")
    claims_df = gen_claims.generate(counts["claims"], rng, pol_sum, ref, config.DQ)
    common.write_ndjson_chunks(claims_df, out / "claims", "claims", config.ROWS_PER_FILE["claims"])

    log("payments (installments per policy)")
    pay_df = gen_payments.generate(rng, pol_sum, config.DQ)
    common.write_parquet_chunks(pay_df, out / "payments", "payments", config.ROWS_PER_FILE["payments"])

    auto = pol_sum[pol_sum["is_auto"]]
    target_bytes = target_mb * 1024 * 1024
    log(f"telematics -> fill landing to {target_mb} MB")
    ev, _ = _fill_telematics(out, target_bytes, rng, auto["policy_id"].to_numpy(),
                             auto["customer_id"].to_numpy(), 1, 0, "telematics", False, 0, log)

    manifest = {
        "mode": "initial",
        "seed": seed,
        "target_mb": target_mb,
        "counts": {**counts, "payments": int(len(pay_df)), "telematics_events": int(ev - 1)},
        "id_ranges": {
            "customers": [1, counts["customers"]],
            "agents": [1, counts["agents"]],
            "policies": [1, counts["policies"]],
            "claims": [1, counts["claims"]],
        },
        "size_mb": round(common.dir_size_bytes(out) / 1024 / 1024, 1),
    }
    common.write_manifest(out, manifest)
    return manifest


def generate_delta(out_dir, batch, seed, log) -> dict:
    out = Path(out_dir)
    man = common.read_manifest(out)
    pre = common.dir_size_bytes(out)  # landing size before this batch (for delta sizing)
    base = (man or {}).get("counts", config.BASE_COUNTS)
    cust_n = int(base["customers"])
    agt_n = int(base["agents"])
    pol_n = int(base["policies"])

    rng = common.make_rng(seed + 1000 + batch)
    pools = common.build_pools(common.make_faker(seed + 1000 + batch), 6000, 6000, 4000, 2000)
    ref = _prepare_reference()
    dq = config.DQ
    dc = config.DELTA_COUNTS
    drift = batch == config.SCHEMA_DRIFT_BATCH

    existing_cust = common.make_ids("CUST", 1, cust_n, 8)
    existing_agt = common.make_ids("AGT", 1, agt_n, 5)
    new_pol_start = pol_n + (batch - 1) * dc["policies"] + 1

    # ---- customer updates: existing ids, newer updated_at -> SCD2 / MERGE source ----
    log(f"customer updates x{dc['customers_updates']:,}")
    upd = gen_customers.generate(dc["customers_updates"], rng, pools, ref, dq)
    upd = upd.iloc[: dc["customers_updates"]].copy()
    upd["customer_id"] = existing_cust[rng.integers(0, cust_n, len(upd))]
    upd["updated_at"] = common.iso_ts(common.random_datetimes(rng, "2026-05-04", "2026-06-30", len(upd)))
    common.write_csv_chunks(upd, out / "customers", f"customers_delta{batch}", config.ROWS_PER_FILE["customers"])

    # ---- agent updates: existing ids, changed branch/status, newer updated_at ----
    log(f"agent updates x{dc['agents_updates']:,}")
    aupd = gen_agents.generate(dc["agents_updates"], rng, pools, ref, dq)
    aupd["agent_id"] = existing_agt[rng.integers(0, agt_n, len(aupd))]
    aupd["status"] = "INACTIVE"
    aupd["updated_at"] = common.iso_ts(common.random_datetimes(rng, "2026-05-04", "2026-06-30", len(aupd)))
    common.write_csv_chunks(aupd, out / "agents", f"agents_delta{batch}", config.ROWS_PER_FILE["agents"])

    # ---- new policies / claims / payments ----
    log(f"new policies x{dc['policies']:,}")
    pol_records, pol_sum = gen_policies.generate(
        dc["policies"], rng, existing_cust, existing_agt, ref, dq, id_start=new_pol_start
    )
    common.write_json_array_chunks(pol_records, out / "policies", f"policies_delta{batch}", config.ROWS_PER_FILE["policies"])

    log(f"new claims x{dc['claims']:,}")
    claims_df = gen_claims.generate(dc["claims"], rng, pol_sum, ref, dq, id_start=(batch * 10_000_000 + 1))
    common.write_ndjson_chunks(claims_df, out / "claims", f"claims_delta{batch}", config.ROWS_PER_FILE["claims"])

    log("new payments")
    pay_df = gen_payments.generate(rng, pol_sum, dq, id_start=(batch * 100_000_000 + 1))
    common.write_parquet_chunks(pay_df, out / "payments", f"payments_delta{batch}", config.ROWS_PER_FILE["payments"])

    # ---- telematics fill ~DELTA_TARGET_MB (device_fw appears on the drift batch) ----
    # Telematics tops the batch up so the WHOLE delta (new business + telematics)
    # lands near DELTA_TARGET_MB, not DELTA_TARGET_MB on top of the other files.
    auto = pol_sum[pol_sum["is_auto"]]
    baseline = common.dir_size_bytes(out)
    other_bytes = baseline - pre
    tele_target = max(config.DELTA_TARGET_MB * 1024 * 1024 - other_bytes, 5 * 1024 * 1024)
    log(f"telematics delta -> batch total ~{config.DELTA_TARGET_MB} MB (device_fw={'yes' if drift else 'no'})")
    ev, _ = _fill_telematics(
        out, tele_target, rng,
        auto["policy_id"].to_numpy(), auto["customer_id"].to_numpy(),
        batch * 100_000_000 + 1, 0, f"telematics_delta{batch}", drift, baseline, log,
    )

    return {
        "mode": "delta",
        "batch": batch,
        "schema_drift": drift,
        "new_policy_id_start": new_pol_start,
        "delta_size_mb": round((common.dir_size_bytes(out) - pre) / 1024 / 1024, 1),
        "size_mb": round(common.dir_size_bytes(out) / 1024 / 1024, 1),
    }


def main():
    ap = argparse.ArgumentParser(description="Insurance synthetic-data generator")
    ap.add_argument("--mode", choices=["initial", "delta"], default="initial")
    ap.add_argument("--batch", type=int, default=1, help="delta batch number (>=1)")
    ap.add_argument("--target-mb", type=int, default=config.TARGET_MB_DEFAULT)
    ap.add_argument("--out", default=config.DEFAULT_OUT)
    ap.add_argument("--seed", type=int, default=config.MASTER_SEED)
    args = ap.parse_args()

    t0 = time.time()

    def log(msg):
        print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

    if args.mode == "initial":
        result = generate_initial(args.out, args.target_mb, args.seed, log)
    else:
        result = generate_delta(args.out, args.batch, args.seed, log)

    log("done")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

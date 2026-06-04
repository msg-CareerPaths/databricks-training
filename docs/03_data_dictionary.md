# 03 · Data Dictionary

The generator (`data_generator/`) lands **seven sources** under the UC Volume
`insurance.landing.raw`, one folder each. Sources are intentionally **dirty** — every
defect below maps to a cleaning task you must implement in the **silver** layer (M3).

| Source | Folder | Format | Initial scale | Ingestion taught (M2) |
|---|---|---|---|---|
| Customers | `customers/` | CSV (header) | ~250k rows | Auto Loader + schema evolution |
| Policies | `policies/` | JSON array, nested `coverages[]` | ~300k rows | Auto Loader / `read_files` (`multiLine`) |
| Claims | `claims/` | JSON (newline-delimited) | ~180k rows | Auto Loader |
| Payments | `payments/` | Parquet | ~1.7M rows | **COPY INTO** |
| Agents | `agents/` | CSV (header) | ~3k rows | batch / Auto Loader (SCD2 source) |
| Telematics | `telematics/` | JSONL | ~0.7M+ events | **Auto Loader streaming** (late data) |
| Reference | `reference/` | CSV (committed) | 6 small files | batch `read_files` |

> Sizes scale with `--target-mb`. Each source is written as **multiple files** so Auto
> Loader has discrete files to discover and COPY INTO has multiple inputs. Deltas append
> `*_delta<N>_*` files (and, on batch 2, the `telematics.device_fw` schema-drift column).

---

## Source schemas

### `customers` (CSV) — CRM export, PII
| Column | Type (raw) | Notes |
|---|---|---|
| customer_id | string | Business key, `CUST00000001` |
| first_name, last_name | string | |
| email | string | **nullable defect**; some malformed |
| phone | string | **nullable defect** |
| address_line1, city | string | `city` has **casing/whitespace** noise |
| state | string | 2-letter; **invalid/typo** values (`ca`, `Calif`, `XX`) → map via `ref_us_states` |
| postal_code | string | 5-digit; first digit joins `ref_postal_region.zip1` → region |
| date_of_birth | string | **mixed/ malformed/ future** date formats |
| gender | string | `M` / `F` / `X` |
| customer_since | string (date) | |
| segment | string | `PERSONAL` / `COMMERCIAL` |
| created_at, updated_at | string (ts) | `updated_at` drives MERGE/SCD2 from deltas |

### `policies` (JSON, nested) — policy admin system
| Field | Type | Notes |
|---|---|---|
| policy_id | string | `POL000000001` |
| customer_id | string | FK → customers; **orphan defect** (~1%) |
| agent_id | string | FK → agents |
| product_line | string | `AUTO` / `PROPERTY` |
| status | string | `ACTIVE`/`CANCELLED`/`LAPSED`/`PENDING`; **typos** |
| effective_date, expiration_date | string (date) | `effective_date` has **bad-date** noise |
| annual_premium | number **or string** | **negative** + **numeric-as-string** (`"$1,234.50"`) defects |
| sum_insured | number | used to detect `loss_amount > sum_insured` |
| payment_frequency | string | `ANNUAL`/`SEMI_ANNUAL`/`QUARTERLY`/`MONTHLY` → installment count |
| coverages | array<struct> | `{coverage_code, limit, deductible, peril_code}` — **explode in silver** |
| vehicle | struct | AUTO only: `{make, model, year, vin, vehicle_type}` |
| property | struct | PROPERTY only: `{construction, year_built, sq_ft}` |

### `claims` (JSON ndjson) — claims system
| Field | Type | Notes |
|---|---|---|
| claim_id | string | `CLM000000001` |
| policy_id | string | FK → policies; **orphan defect** |
| customer_id | string | denormalized FK |
| loss_date, claim_date | string (date) | `claim_date` has **bad-date** noise |
| peril_code | string | FK → `ref_peril_codes`; **nullable** |
| claim_status | string | `OPEN`/`CLOSED`/`DENIED`/`IN_REVIEW`; **typos** → map via `ref_claim_status` |
| reported_amount | number | **nullable** |
| loss_amount | number | some **> sum_insured** (out-of-range) |
| paid_amount | number | non-zero mainly when `CLOSED` |
| fraud_flag | string | **mixed representation**: `true/false`, `Y/N`, `1/0`, `YES/NO` → normalize to boolean |
| description | string | free text |

### `payments` (Parquet) — finance/billing export
| Column | Type | Notes |
|---|---|---|
| payment_id | string | `PAY0000000001` |
| policy_id | string | FK → policies |
| installment_no | int | 1..N by `payment_frequency` |
| due_date | date | |
| paid_date | date | **null** when MISSED or PENDING |
| amount_due | double | some **negative** (out-of-range) |
| amount_paid | double | **null** when MISSED/PENDING; may differ from `amount_due` |
| payment_method | string | `ACH`/`CARD`/`CHECK`/`CASH` |
| status | string | `PAID`/`LATE`/`MISSED`/`PENDING` (derived from dates) |

### `agents` (CSV) — SCD2 dimension source
`agent_id`, `first_name`, `last_name`, `email` (nullable), `branch` (casing noise),
`region`, `state`, `hire_date`, `status` (`ACTIVE`/`INACTIVE`), `commission_rate`,
`updated_at`. Deltas land **updates** (same `agent_id`, newer `updated_at`, often
`status=INACTIVE`) → fold into Type-2 history (see `src/silver/clean_agents_scd2.py`).

### `telematics` (JSONL) — per-trip IoT events
`event_id`, `trip_id`, `policy_id` (FK, AUTO), `customer_id`, `event_ts` (**late /
out-of-order** → watermark), `latitude`/`longitude`/`speed_kmh` (**nullable**;
`speed_kmh` has **out-of-range** negatives / >300), `heading`, `harsh_brake`,
`harsh_accel`, `harsh_corner`, `mileage`, `device_id`, **`device_fw`** *(only in the
schema-drift delta — tests Auto Loader schema evolution)*.

### `reference` (CSV, committed in repo)
`ref_us_states` (`state_code,state_name,region`) · `ref_vehicle_makes`
(`make,model,vehicle_type,body_style`) · `ref_peril_codes`
(`peril_code,peril_name,product_line`) · `ref_coverage_types`
(`coverage_code,coverage_name,product_line`) · `ref_claim_status`
(`status_code,status_name`) · `ref_postal_region` (`zip1,region`).

---

## Seeded data-quality defect catalogue → silver fix

| Defect | Where it appears | How you clean it (silver) | Exam domain |
|---|---|---|---|
| Exact duplicate rows | customers, claims | `dropDuplicates()` / `DISTINCT` | 3 |
| Fuzzy duplicates (same key, casing/space) | customers | normalize text, then dedupe by business key | 3 |
| Nulls in required fields | customers, claims, telematics | expectations + **quarantine** pattern | 3 |
| Inconsistent casing / whitespace | customers.city, agents.branch | `trim` / `initcap` / `upper` | 3 |
| Invalid categoricals / typos | customers.state, policies.status, claims.claim_status | map/validate via reference tables | 3 |
| Out-of-range numerics | policies.annual_premium (neg), claims.loss_amount (> sum_insured), payments.amount_due (neg) | range checks → fix or quarantine | 3 |
| Multi-format / future / malformed dates | customers.date_of_birth, policies.effective_date, claims.claim_date | parse with fallbacks, drop/flag future | 3 |
| Orphan foreign keys | policies.customer_id, claims.policy_id | anti-join vs parent → quarantine | 3 |
| Numeric stored as string | policies.annual_premium | strip symbols, `cast` | 3 |
| Mixed boolean representation | claims.fraud_flag | map `{Y,1,YES,true}`→true | 3 |
| Late / out-of-order events | telematics.event_ts | watermark + window dedupe | 3 |
| **Schema drift** (new column) | telematics.device_fw (delta batch 2) | **Auto Loader `schemaEvolutionMode`** | **2** |

The running tally of quarantined rows per layer feeds the **DQ scorecard**
(`ops.dq_scorecard`, requirement #8).

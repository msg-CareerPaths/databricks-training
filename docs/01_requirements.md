# 01 · Business Requirements

You are the data engineer for a fictional P&C insurer writing **Auto** and **Property**
policies. The business wants a governed lakehouse and a set of dashboards. Each
requirement below becomes a **gold table** (built in M4) and a **dashboard tile**
(built in M10). The acceptance criterion for the project is: *every question is
answerable from a gold table, and the answer is correct after silver cleaning.*

> "Earned premium" simplification for this project: monthly earned premium =
> `annual_premium / 12` accrued over the months a policy is in force. "Incurred losses"
> = `loss_amount` for claims with `loss_date` in the period. Loss ratio = incurred
> losses ÷ earned premium.

| # | Business question | Metric / definition | Target gold table |
|---|---|---|---|
| 1 | **Loss ratio** by product line & state, monthly | incurred losses ÷ earned premium | `gold.agg_loss_ratio` |
| 2 | **Claims frequency & severity** trends; severity by peril | freq = claims ÷ active policies; severity = avg `loss_amount` | `gold.fact_claims`, `gold.agg_claims_monthly` |
| 3 | **Fraud rate** & flagged-dollar exposure over time | fraud claims ÷ claims; Σ `loss_amount` where fraud | `gold.fact_claims` (clean `fraud_flag`) |
| 4 | **Premium written vs collected** & overdue | Σ `amount_due` vs Σ `amount_paid`; overdue = LATE/MISSED | `gold.fact_premium`, `gold.fact_payments` |
| 5 | **Agent performance** | policies sold, retention, loss ratio per agent | `gold.agg_agent_performance` + `gold.dim_agent` |
| 6 | **Telematics risk** vs claims | harsh-event score per policy vs claim likelihood | `gold.agg_telematics_risk` |
| 7 | **Customer 360 / retention** | tenure, active policies, lifetime premium, churn flag | `gold.dim_customer`, `gold.agg_customer_value` |
| 8 | **Data-quality scorecard** | rows quarantined per layer/rule, freshness | `ops.dq_scorecard` |

## Detail & acceptance criteria

1. **Loss ratio** — grain: month × product_line × state. Joins `fact_claims` +
   `fact_premium` + `dim_date` + `dim_policy`. *Accept:* loss ratio is between 0 and a
   sane cap for >95% of cells; states are canonical 2-letter (typos already mapped).

2. **Frequency & severity** — monthly trend lines; severity broken out by `peril_code`
   (joined to `ref_peril_codes` for names). *Accept:* perils resolve to known names; no
   nulls in the peril dimension after cleaning.

3. **Fraud** — `fraud_flag` must be a real boolean (the raw is mixed `Y/N/1/0/...`).
   *Accept:* fraud rate ≈ 4% (the seeded rate) once normalized; no string values remain.

4. **Billing leakage** — written = Σ scheduled `amount_due`; collected = Σ `amount_paid`;
   overdue exposure = Σ `amount_due` for `status IN (LATE, MISSED)`. *Accept:* collected ≤
   written; overdue ≈ 16% of installments (12% late + 4% missed).

5. **Agent performance** — uses the **SCD2** `dim_agent` (current view) for attributes;
   retention = share of policies renewed. *Accept:* every active policy maps to exactly
   one current agent version.

6. **Telematics risk** — per-policy harsh-driving score = weighted harsh_brake/accel/
   corner per 100 mi; compare score quintile vs claim rate. *Accept:* late/out-of-order
   events are de-duplicated via watermark; scores computed only from valid speeds.

7. **Customer 360** — one current row per customer (fuzzy/exact dupes removed),
   with tenure, #active policies, lifetime premium, and a churn flag. *Accept:* customer
   count after dedup < raw row count; no duplicate `customer_id` in `dim_customer`.

8. **DQ scorecard** — for each silver table, count rows passed vs quarantined per rule,
   plus source freshness. *Accept:* totals reconcile (passed + quarantined = ingested).

## Dashboards (M10)

Built on a serverless SQL warehouse (see `dashboards/README.md`):
**Executive Loss & Premium** (1, 4), **Claims & Fraud** (2, 3), **Distribution / Agents**
(5), **Telematics Risk** (6), **Customer & Retention** (7), **Data Quality** (8).

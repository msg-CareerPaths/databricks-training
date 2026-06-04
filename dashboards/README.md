# Dashboards (M10)

Six **AI/BI dashboards** built on a **serverless SQL warehouse**, each answering business
questions from `docs/01_requirements.md` off the **gold** tables. Refresh them on a schedule
or as a **dashboard task** in the Lakeflow Job (`resources/insurance_ingest.job.yml`).
Studybook: `docs/studybook/M10_dashboards_and_readiness.md`.

> Build the gold tables first (M4 stubs in `src/gold/`). Each tile names its gold source.

## 1. Executive Loss & Premium  *(requirements #1, #4)*
| Tile | Gold source | Chart |
|---|---|---|
| Monthly loss ratio by product line | `gold.agg_loss_ratio` | line (series = product_line) |
| Loss ratio heatmap by state | `gold.agg_loss_ratio` | map / heatmap |
| Premium written vs collected | `gold.fact_premium`, `gold.fact_payments` | grouped bar |
| Overdue exposure ($) trend | `gold.fact_payments` | area |

## 2. Claims & Fraud  *(requirements #2, #3)*
| Tile | Gold source | Chart |
|---|---|---|
| Claim frequency & severity trend | `gold.agg_claims_monthly` | dual-axis line |
| Severity by peril | `gold.agg_claims_monthly` | bar |
| Fraud rate over time | `gold.fact_claims` | line |
| Flagged-fraud $ exposure | `gold.fact_claims` | KPI + bar |

## 3. Distribution / Agents  *(requirement #5)*
| Tile | Gold source | Chart |
|---|---|---|
| Policies sold per agent (top N) | `gold.agg_agent_performance` | bar |
| Agent loss ratio vs retention | `gold.agg_agent_performance` | scatter |
| Performance by region | `gold.agg_agent_performance` + `gold.dim_agent` | table |

## 4. Telematics Risk  *(requirement #6)*
| Tile | Gold source | Chart |
|---|---|---|
| Harsh-score distribution | `gold.agg_telematics_risk` | histogram |
| Claim rate by score quintile | `gold.agg_telematics_risk` | bar |

## 5. Customer & Retention  *(requirement #7)*
| Tile | Gold source | Chart |
|---|---|---|
| Active customers & lifetime premium | `gold.agg_customer_value` | KPI |
| Churn flag breakdown | `gold.agg_customer_value` | donut |
| Tenure distribution | `gold.dim_customer` | histogram |

## 6. Data Quality  *(requirement #8)*
| Tile | Gold source | Chart |
|---|---|---|
| Quarantine rate per silver table | `gold.v_dq_scorecard` | bar |
| Rows passed vs quarantined | `gold.v_dq_scorecard` | stacked bar |
| Source freshness (latest ingest) | `gold.v_dq_scorecard` | table |

## Build notes
- Create each tile from a **gold** query (don't query silver/bronze directly from a tile).
- Add a **date filter** (joined to `gold.dim_date`) and a **product_line** filter widget.
- Mask PII in any customer-level tile per `docs/studybook/M9_governance_security.md`.
- The DQ dashboard (#6) is your operational health check — keep it on the job's success path.

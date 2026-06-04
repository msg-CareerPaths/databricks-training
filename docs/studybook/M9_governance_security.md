# M9 · Governance & Security

> **Exam domain 7 — Governance and Security.** Objectives: differentiate **managed vs
> external** tables in Unity Catalog and do basic ops (create/modify/delete/**convert**);
> configure access control via UI **and** SQL — **GRANT / REVOKE / DENY** to principals
> (users, groups, service principals) at the right **security-hierarchy** level
> (`USE CATALOG` → `USE SCHEMA` → `SELECT`/`MODIFY`); apply **column-level masking** and
> **row-level security**; and use **Unity Catalog ABAC policies** for centralized row
> filtering + column masking. Our PII lives in `gold.dim_customer` (email, phone, DOB) —
> this chapter locks it down.

## 1 · Concept / theory

### Unity Catalog secures the whole namespace
Governance in Databricks is **Unity Catalog (UC)**, not table ACLs scattered per workspace.
One **metastore** (per region) governs every workspace attached to it, with the
three-level namespace `catalog.schema.object` (`insurance.gold.dim_customer`). Everything
below — permissions, masks, filters, lineage, audit — is centralized here.

### Managed vs external tables
- **Managed** — UC owns *both* metadata **and** the data files, stored in the metastore's
  (or catalog's/schema's) **managed storage location**. `DROP TABLE` deletes the data.
  Default, simplest, and what **Free Edition uses throughout** this project. Supports
  predictive optimization, easy `UNDROP`.
- **External** — UC owns the *metadata* but the data lives at a **`LOCATION`** you name
  (governed via an **external location** + **storage credential**). `DROP TABLE` removes
  only the registration; the **files stay**. Use when data is shared with non-Databricks
  tools or must live at a fixed path.
- **Convert between them** — `ALTER TABLE … SET LOCATION` and the managed↔external
  conversion path move a table across the boundary (worked example below). External
  locations are **not creatable on Free Edition** (no storage credentials), so you learn
  the syntax here and use **managed** tables hands-on.

### The security hierarchy (privileges are not flat)
To read `insurance.gold.dim_customer`, a principal needs the **whole chain**:
`USE CATALOG insurance` → `USE SCHEMA insurance.gold` → `SELECT` on the object. `USE …`
grants *traversal* (the ability to "see into"), not data access; `SELECT`/`MODIFY` grant
the data action. Privileges **inherit downward**: `GRANT SELECT ON SCHEMA insurance.gold`
covers every current and future table in that schema. The common bug is granting `SELECT`
on a table but forgetting `USE CATALOG`/`USE SCHEMA` — the user still gets *permission
denied*.

### Principals: users, groups, service principals
Grant to **groups**, almost never individuals — add/remove people from the group instead
of re-running grants. **Service principals** are non-human identities (for jobs/CI). The
**object owner** and metastore/account admins have implicit rights.

### GRANT / REVOKE / DENY
- **GRANT** gives a privilege. **REVOKE** removes a previously granted one.
- **DENY** is an *explicit block* that **overrides** any GRANT (including inherited ones) —
  e.g. let the analyst group read all of `gold` **except** `dim_customer`. DENY is the only
  way to carve an exception out of an inherited grant.

### Column-level masking & row-level security
Two complementary fine-grained controls, defined as **UDFs** attached to a table:
- **Column mask** — a function applied to a column at query time; it returns the real value
  for privileged callers and a masked value (`***`, `NULL`, hash) for everyone else. Uses
  `is_account_group_member()` / `current_user()` to branch. Masks `dim_customer.email` and
  `.phone`.
- **Row filter** — a boolean function attached to a table that **drops rows** the caller may
  not see. Agents see only rows for *their* `region`/`state`. The filter runs before results
  are returned, so unauthorized rows never leave UC.
Both are **enforced centrally** regardless of how the table is queried (SQL, PySpark,
dashboard, BI tool).

### ABAC policies (attribute-based access control)
Per-table mask/filter functions don't scale — 12 PII columns across 6 tables = lots of
hand-wiring. **ABAC** inverts this: you **tag** columns/tables with governed **tags** (e.g.
`pii = email`), define **one policy** that says *"mask any column tagged `pii` for users not
in `pii_readers`"*, and UC applies it **automatically** everywhere that tag appears —
present tables and future ones. Same idea for row filters via tags. Tag once, govern
centrally; the policy is the single source of truth instead of N copy-pasted functions.
*(ABAC is the newest governance feature; confirm current availability on Free Edition.)*

### Lineage & audit
UC auto-captures **lineage** (table- and column-level: which job/notebook produced
`gold.dim_customer`, which dashboards consume it) and writes an **audit log** of every
action (grants, queries, access-denied). On UC these surface as queryable **system tables**
under the `system` catalog — `system.access.audit`, `system.access.table_lineage`,
`system.access.column_lineage` — the exam's "how do I see who accessed PII / where did this
column come from" answer.

## 2 · Worked code (SQL)

**A · Grant an analyst group read access to gold (the full hierarchy):**
```sql
-- traversal down the namespace, then the data privilege on the whole schema
GRANT USE CATALOG ON CATALOG insurance              TO `insurance_analysts`;
GRANT USE SCHEMA  ON SCHEMA  insurance.gold          TO `insurance_analysts`;
GRANT SELECT      ON SCHEMA  insurance.gold          TO `insurance_analysts`;  -- inherits to all tables

-- writers (ETL service principal) also need MODIFY + the ability to create
GRANT USE SCHEMA, CREATE, MODIFY ON SCHEMA insurance.silver TO `etl_sp`;

-- carve an exception: analysts read all gold EXCEPT the PII dimension
DENY  SELECT ON TABLE insurance.gold.dim_customer    TO `insurance_analysts`;  -- overrides the inherited GRANT

REVOKE SELECT ON SCHEMA insurance.gold FROM `interns`;   -- remove a prior grant
SHOW GRANTS `insurance_analysts` ON SCHEMA insurance.gold;
```

**B · Column mask on PII — `dim_customer.email` / `.phone`:**
```sql
-- privileged readers see the real value; everyone else sees a redacted form
CREATE OR REPLACE FUNCTION insurance.gold.mask_email(v STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN v
            ELSE regexp_replace(v, '^.*@', '***@') END;   -- keep domain, hide local part

CREATE OR REPLACE FUNCTION insurance.gold.mask_phone(v STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN v
            ELSE 'XXX-XXX-' || right(v, 4) END;

-- attach the masks (also doable at CREATE TABLE with MASK clauses)
ALTER TABLE insurance.gold.dim_customer
  ALTER COLUMN email SET MASK insurance.gold.mask_email;
ALTER TABLE insurance.gold.dim_customer
  ALTER COLUMN phone SET MASK insurance.gold.mask_phone;
-- (to drop: ALTER TABLE ... ALTER COLUMN email DROP MASK;)
```

**C · Row-level security — agents see only their `region`:**
```sql
-- TRUE = row is visible. Privileged groups see all; otherwise match the caller's region.
CREATE OR REPLACE FUNCTION insurance.gold.region_filter(region STRING)
RETURN is_account_group_member('claims_admins')
       OR region = current_user_region();   -- a lookup mapping the signed-in user → region

ALTER TABLE insurance.gold.dim_agent
  SET ROW FILTER insurance.gold.region_filter ON (region);
-- the same pattern filters fact_claims / fact_premium on state for territory-scoped agents
-- (to drop: ALTER TABLE insurance.gold.dim_agent DROP ROW FILTER;)
```

**D · ABAC — tag once, one policy masks everywhere (vs per-table functions):**
```sql
-- 1) tag the sensitive columns instead of wiring a function per column
ALTER TABLE insurance.gold.dim_customer ALTER COLUMN email         SET TAGS ('pii' = 'email');
ALTER TABLE insurance.gold.dim_customer ALTER COLUMN phone         SET TAGS ('pii' = 'phone');
ALTER TABLE insurance.gold.dim_customer ALTER COLUMN date_of_birth SET TAGS ('pii' = 'dob');

-- 2) ONE centrally-managed policy applies the mask wherever the `pii` tag appears
--    (present and future tables) — no per-table ALTER, no copy-pasted UDFs.
CREATE OR REPLACE POLICY mask_all_pii
  ON CATALOG insurance
  COMMENT 'Redact any PII-tagged column for non-privileged users'
  COLUMN MASK insurance.gold.mask_string
  TO `account users` EXCEPT `pii_readers`
  MATCH COLUMNS hasTagValue('pii', 'email') AS v;        -- one policy ≫ many functions
```

**E · Managed ↔ external (create, convert, and the drop difference):**
```sql
-- managed (the FE default): UC owns the files; DROP deletes data
CREATE TABLE insurance.gold.dim_policy (policy_id STRING, product_line STRING);

-- external: UC owns metadata only; files live at a governed LOCATION; DROP keeps files
CREATE TABLE insurance.gold.dim_policy_ext (policy_id STRING, product_line STRING)
USING DELTA LOCATION 's3://insurance-lake/gold/dim_policy_ext';   -- needs an external location

-- convert managed -> external by repointing storage (and the reverse via SET LOCATION back)
ALTER TABLE insurance.gold.dim_policy SET LOCATION 's3://insurance-lake/gold/dim_policy';
DESCRIBE EXTENDED insurance.gold.dim_policy;   -- 'Type' shows MANAGED vs EXTERNAL + Location
```

## 3 · Best practices & pitfalls
- **Grant to groups, not users** — and use service principals for jobs/CI; never embed a
  human's token in a pipeline.
- **Mind the hierarchy** — `SELECT` without `USE CATALOG` + `USE SCHEMA` = *permission
  denied*. The missing `USE …` is the #1 governance gotcha.
- **DENY beats GRANT** — it's the only way to subtract from an inherited grant (analysts
  read `gold`, deny `dim_customer`). Don't expect a later GRANT to "undo" a DENY.
- **Don't hand-roll PII protection** in the query layer — UC masks/filters enforce
  centrally across SQL, PySpark, dashboards, and external BI; a `CASE` in one dashboard
  protects nothing else.
- **Prefer ABAC at scale** — tag-driven policies beat N per-table functions for many PII
  columns; per-table masks are fine for one-off needs.
- **Managed vs external `DROP`** — dropping a **managed** table deletes the data (use
  `UNDROP` fast); dropping an **external** table leaves files behind. Know which you have
  (`DESCRIBE EXTENDED`).
- **On Free Edition** you use **managed** tables only (no storage credentials/external
  locations); ABAC and external conversions may be limited — learn the syntax, verify
  availability before relying on it.
- **Audit via system tables** — answer "who read PII?" with `system.access.audit`, not
  guesswork; lineage answers "where did this column come from?".

## 4 · Exam focus
**Objectives tested:** managed vs external tables + basic/convert ops; access control via
UI and SQL (`GRANT`/`REVOKE`/`DENY` to users/groups/service principals at the right
hierarchy level — `USE CATALOG`→`USE SCHEMA`→`SELECT`/`MODIFY`); column masking; row-level
security; **UC ABAC** for centralized row filtering + column masking; lineage/audit.

**Practice questions**
1. *An analyst group can query most of `insurance.gold` but **must never** see
   `dim_customer`. You already ran `GRANT SELECT ON SCHEMA insurance.gold TO
   insurance_analysts`. What's the minimal next step?*
   **A.** `DENY SELECT ON TABLE insurance.gold.dim_customer TO insurance_analysts`. DENY
   **overrides** the inherited schema-level GRANT. (Revoking the schema grant would also
   block the rest of gold; a second GRANT can't subtract one table.)

2. *A new user gets `GRANT SELECT ON TABLE insurance.gold.fact_claims` but still hits
   "permission denied". Why?*
   **A.** They lack **`USE CATALOG insurance`** and/or **`USE SCHEMA insurance.gold`** —
   `SELECT` alone doesn't grant namespace traversal; the full hierarchy is required.

3. *You must redact `email`, `phone`, and `date_of_birth` across `dim_customer` **and**
   future tables for everyone outside `pii_readers`, managed in one place. Best approach?*
   **A.** **UC ABAC** — tag the columns `pii` and define **one column-mask policy** keyed
   on the tag; it applies automatically to present and future tagged columns. (Per-table
   `SET MASK` functions work but must be re-wired on every table — not centralized.)

4. *You `DROP TABLE` an **external** table by mistake. Where's the data?*
   **A.** Still at its `LOCATION` — dropping an external table removes only the UC
   registration; re-create it pointing at the same path. (Dropping a **managed** table
   deletes the files — recover quickly with `UNDROP TABLE`.)

## 5 · References
- Unity Catalog **privileges & securable objects**; `GRANT` / `REVOKE` / `DENY`; the
  `USE CATALOG` / `USE SCHEMA` / `SELECT` / `MODIFY` hierarchy and inheritance
- **Managed vs external tables**; `CREATE TABLE … LOCATION`, `ALTER TABLE … SET LOCATION`,
  managed↔external conversion; external locations & storage credentials
- **Column masks** (`ALTER COLUMN … SET MASK`) and **row filters** (`SET ROW FILTER`);
  `is_account_group_member()` / `current_user()`
- **Attribute-based access control (ABAC)** policies; governed tags
- **Lineage** (table/column) and **audit** via **system tables** (`system.access.*`)
- Free Edition governance limitations

*(Look these up on docs.databricks.com — the exam tracks the current docs.)*

# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Engine — Simple Metadata-Driven Data Quality Framework
# MAGIC **Run all cells: Run → Run All**

# COMMAND ----------
# MAGIC %md ## CELL 1 — Setup: Create Catalog & Schemas

# COMMAND ----------

CATALOG = "dq_fw"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.config")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.results")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.staging")

print(f"Catalog '{CATALOG}' and schemas ready.")

# COMMAND ----------
# MAGIC %md ## CELL 2 — Create Config Tables: dq_rules + dq_mapping

# COMMAND ----------

# Drop for clean re-run
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.config.dq_rules")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.config.dq_mapping")

# ── dq_rules: the rule library ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE {CATALOG}.config.dq_rules (
    rule_id     INT,
    rule_name   STRING,
    rule_type   STRING,
    rule_sql    STRING,
    severity    STRING,
    is_active   BOOLEAN
) USING DELTA
""")

# ── dq_mapping: rule → table + column binding ───────────────────────────────
spark.sql(f"""
CREATE TABLE {CATALOG}.config.dq_mapping (
    mapping_id       INT,
    rule_id          INT,
    table_catalog    STRING,
    table_schema     STRING,
    table_name       STRING,
    column_name      STRING,
    filter_condition STRING,
    pk_columns       STRING,
    rule_params      STRING,
    threshold        DOUBLE,
    is_active        BOOLEAN
) USING DELTA
""")

print("Config tables created: dq_rules, dq_mapping")

# COMMAND ----------
# MAGIC %md ## CELL 3 — Create Results Tables: dq_run_results, dq_failed_records, dq_score

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.results.dq_run_results")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.results.dq_failed_records")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.results.dq_score")

# ── dq_run_results: one row per rule per run ────────────────────────────────
spark.sql(f"""
CREATE TABLE {CATALOG}.results.dq_run_results (
    run_id          STRING,
    run_date        DATE,
    mapping_id      INT,
    rule_name       STRING,
    table_catalog   STRING,
    table_schema    STRING,
    table_name      STRING,
    column_name     STRING,
    total_records   LONG,
    failed_records  LONG,
    pass_rate       DOUBLE,
    threshold       DOUBLE,
    status          STRING,
    severity        STRING,
    error_msg       STRING
) USING DELTA
PARTITIONED BY (run_date, table_catalog)
""")

# ── dq_failed_records: actual bad rows ─────────────────────────────────────
spark.sql(f"""
CREATE TABLE {CATALOG}.results.dq_failed_records (
    run_id      STRING,
    run_date    DATE,
    rule_name   STRING,
    table_name  STRING,
    column_name STRING,
    pk_value    STRING,
    bad_value   STRING,
    severity    STRING
) USING DELTA
PARTITIONED BY (run_date)
""")

# ── dq_score: summary per table per run ─────────────────────────────────────
spark.sql(f"""
CREATE TABLE {CATALOG}.results.dq_score (
    run_id        STRING,
    run_date      DATE,
    table_catalog STRING,
    table_schema  STRING,
    table_name    STRING,
    total_rules   INT,
    passed        INT,
    failed        INT,
    errors        INT,
    dq_score_pct  DOUBLE
) USING DELTA
PARTITIONED BY (run_date)
""")

print("Results tables created: dq_run_results, dq_failed_records, dq_score")

# COMMAND ----------
# MAGIC %md ## CELL 4 — Create Sample Source Tables (Customer + Sales)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.staging.customer")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.staging.sales")

# customer — contains intentional bad data to trigger DQ failures
cust = [
    (1,  "Alice",  "alice@example.com",  30,  75000.0, "F"),
    (2,  "Bob",    "bob@corp.com",       45,  92000.0, "M"),
    (3,  "Carol",  None,                 28,  60000.0, "F"),   # NULL email
    (4,  "Dave",   "not-an-email",       -5, 120000.0, "M"),   # bad email + age<0
    (5,  "Eve",    "eve@example.com",    35,  85000.0, "F"),
    (6,  "Frank",  "frank@corp.com",     52, 200000.0, "M"),
    (7,  "Grace",  "",                   29,  70000.0, "F"),   # empty email
    (8,  "Harry",  "harry@corp.com",    200,   -500.0, "X"),   # age>120, salary<0, bad gender
    (9,  "Iris",   "iris@example.com",   33,  88000.0, "F"),
    (10, "Jack",   "jack@corp.com",      41,  95000.0, "M"),
    (10, "Jack",   "jack@corp.com",      41,  95000.0, "M"),   # duplicate customer_id
    (11, None,     None,                None,    None,  None), # all nulls
]
cust_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("email",       StringType(),  True),
    StructField("age",         IntegerType(), True),
    StructField("salary",      DoubleType(),  True),
    StructField("gender",      StringType(),  True),
])
(spark.createDataFrame(cust, cust_schema)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.staging.customer"))

# sales — contains bad FKs and negative amounts
sales = [
    (1001, 1,  250.0,  "USD"),
    (1002, 2,  530.0,  "USD"),
    (1003, 99, 120.0,  "USD"),  # customer_id=99 doesn't exist
    (1004, 5,  890.0,  "USD"),
    (1005, 6,  None,   "USD"),  # NULL amount
    (1006, 7,  -50.0,  "USD"),  # negative amount
    (1007, 9,  1200.0, "USD"),
    (1008, 10, 330.0,  "USD"),
]
sales_schema = StructType([
    StructField("sale_id",     IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  True),
])
(spark.createDataFrame(sales, sales_schema)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.staging.sales"))

print(f"staging.customer : {spark.table(f'{CATALOG}.staging.customer').count()} rows")
print(f"staging.sales    : {spark.table(f'{CATALOG}.staging.sales').count()} rows")

# COMMAND ----------
# MAGIC %md ## CELL 5 — Seed Config: Insert Rules & Mappings

# COMMAND ----------

# ── Insert rules ─────────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO {CATALOG}.config.dq_rules VALUES
(1,  'NULL_CHECK',      'COMPLETENESS', '{{col}} IS NOT NULL',                                                         'HIGH',   true),
(2,  'NOT_BLANK',       'COMPLETENESS', '{{col}} IS NOT NULL AND trim({{col}}) != ""',                                 'MEDIUM', true),
(3,  'UNIQUE',          'UNIQUENESS',   '{{col}}',                                                                     'HIGH',   true),
(4,  'EMAIL_FORMAT',    'PATTERN',      '{{col}} RLIKE "^[A-Za-z0-9._%+\\\\-]+@[A-Za-z0-9.\\\\-]+\\\\.[A-Za-z]{{2,}}$"', 'HIGH', true),
(5,  'RANGE_CHECK',     'RANGE',        '{{col}} BETWEEN {{min}} AND {{max}}',                                         'HIGH',   true),
(6,  'NOT_NEGATIVE',    'RANGE',        '{{col}} >= 0',                                                                'HIGH',   true),
(7,  'DOMAIN_CHECK',    'DOMAIN',       '{{col}} IN ({{allowed}})',                                                    'HIGH',   true),
(8,  'FK_CHECK',        'REF_INTEGRITY','{{col}}',                                                                     'HIGH',   true),
(9,  'CUSTOM_SQL',      'CUSTOM_SQL',   '{{sql}}',                                                                    'HIGH',   true)
""")

# ── Insert mappings ──────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO {CATALOG}.config.dq_mapping VALUES
-- CUSTOMER table
(1,  1, '{CATALOG}','staging','customer','email',       NULL, 'customer_id', NULL,                                 0.95, true),
(2,  2, '{CATALOG}','staging','customer','email',       NULL, 'customer_id', NULL,                                 0.95, true),
(3,  4, '{CATALOG}','staging','customer','email',       NULL, 'customer_id', NULL,                                 0.90, true),
(4,  1, '{CATALOG}','staging','customer','name',        NULL, 'customer_id', NULL,                                 0.95, true),
(5,  3, '{CATALOG}','staging','customer','customer_id', NULL, 'customer_id', NULL,                                 1.00, true),
(6,  5, '{CATALOG}','staging','customer','age',         NULL, 'customer_id', '{{"min":"0","max":"120"}}',          0.95, true),
(7,  6, '{CATALOG}','staging','customer','salary',      NULL, 'customer_id', NULL,                                 0.95, true),
(8,  7, '{CATALOG}','staging','customer','gender',      NULL, 'customer_id', '{{"allowed":"''M'',''F'',''O''"}}',  0.95, true),
-- SALES table
(9,  1, '{CATALOG}','staging','sales',   'amount',      NULL, 'sale_id',     NULL,                                 0.95, true),
(10, 6, '{CATALOG}','staging','sales',   'amount',      NULL, 'sale_id',     NULL,                                 0.95, true),
(11, 8, '{CATALOG}','staging','sales',   'customer_id', NULL, 'sale_id',     '{{"ref_catalog":"{CATALOG}","ref_schema":"staging","ref_table":"customer","ref_col":"customer_id"}}', 1.00, true),
(12, 9, '{CATALOG}','staging','sales',    NULL,         NULL, 'sale_id',     '{{"sql":"amount > 0 OR currency IS NOT NULL"}}', 0.95, true)
""")

print(f"dq_rules   : {spark.table(f'{CATALOG}.config.dq_rules').count()} rules")
print(f"dq_mapping : {spark.table(f'{CATALOG}.config.dq_mapping').count()} active mappings")

# COMMAND ----------
# MAGIC %md ## CELL 6 — DQ Engine (Validators)

# COMMAND ----------

import json, uuid
from datetime import date, datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from collections import defaultdict

def run_completeness(df, col, rule_sql):
    expr = rule_sql.replace("{col}", col)
    bad  = df.filter(f"NOT ({expr})")
    return df.count(), bad.count(), bad

def run_uniqueness(df, col):
    w   = Window.partitionBy(col)
    bad = df.withColumn("_n", F.count("*").over(w)).filter("_n > 1").drop("_n")
    return df.count(), bad.count(), bad

def run_pattern(df, col, rule_sql):
    expr = rule_sql.replace("{col}", col)
    base = df.filter(F.col(col).isNotNull())          # skip NULLs for regex
    bad  = base.filter(f"NOT ({expr})")
    return df.count(), bad.count(), bad

def run_range(df, col, rule_sql, params):
    expr = rule_sql.replace("{col}", col)
    for k, v in params.items():
        expr = expr.replace(f"{{{k}}}", str(v))
    base = df.filter(F.col(col).isNotNull())
    bad  = base.filter(f"NOT ({expr})")
    return df.count(), bad.count(), bad

def run_domain(df, col, rule_sql, params):
    allowed = params.get("allowed", "")
    expr    = rule_sql.replace("{col}", col).replace("{allowed}", allowed)
    base    = df.filter(F.col(col).isNotNull())
    bad     = base.filter(f"NOT ({expr})")
    return df.count(), bad.count(), bad

def run_ref_integrity(df, col, params):
    ref_fq  = f"{params['ref_catalog']}.{params['ref_schema']}.{params['ref_table']}"
    ref_col = params["ref_col"]
    ref_df  = spark.table(ref_fq).select(F.col(ref_col).alias("_ref")).distinct()
    src     = df.filter(F.col(col).isNotNull())
    bad     = src.join(ref_df, src[col] == ref_df["_ref"], "left_anti")
    return df.count(), bad.count(), bad

def run_custom_sql(df, params):
    sql_expr = params.get("sql", "1=1")
    bad      = df.filter(f"NOT ({sql_expr})")
    return df.count(), bad.count(), bad

print("DQ Engine validators loaded.")

# COMMAND ----------
# MAGIC %md ## CELL 7 — Run DQ Engine

# COMMAND ----------

RUN_ID   = f"run-{str(uuid.uuid4())[:8]}"
RUN_DATE = date.today()

# ── Load metadata ─────────────────────────────────────────────────────────────
rules_map = {r.rule_id: r.asDict()
             for r in spark.table(f"{CATALOG}.config.dq_rules").filter("is_active=true").collect()}

mappings  = [r.asDict()
             for r in spark.table(f"{CATALOG}.config.dq_mapping").filter("is_active=true")
                           .orderBy("table_name").collect()]

# ── Cache source DataFrames ───────────────────────────────────────────────────
df_cache = {}

result_rows  = []   # → dq_run_results
failed_rows  = []   # → dq_failed_records

print(f"RUN_ID = {RUN_ID}\n{'='*65}")

for m in mappings:
    rule    = rules_map.get(m["rule_id"])
    if not rule:
        print(f"  [SKIP] mapping_id={m['mapping_id']} — rule not found")
        continue

    tbl_fq  = f"{m['table_catalog']}.{m['table_schema']}.{m['table_name']}"
    col     = m["column_name"]
    pk_col  = m["pk_columns"]
    params  = json.loads(m["rule_params"]) if m["rule_params"] else {}
    thresh  = m["threshold"]
    sev     = rule["severity"]
    rtype   = rule["rule_type"]
    rname   = rule["rule_name"]
    rsql    = rule["rule_sql"]

    # Read source once per table (cached)
    if tbl_fq not in df_cache:
        df_cache[tbl_fq] = spark.table(tbl_fq).cache()
        df_cache[tbl_fq].count()
    df = df_cache[tbl_fq]

    # Apply optional filter
    if m.get("filter_condition"):
        df = df.filter(m["filter_condition"])

    # ── Execute the right validator ──────────────────────────────────────────
    total = failed = 0
    bad_df = None
    error_msg = None

    try:
        if rtype == "COMPLETENESS":
            total, failed, bad_df = run_completeness(df, col, rsql)
        elif rtype == "UNIQUENESS":
            total, failed, bad_df = run_uniqueness(df, col)
        elif rtype == "PATTERN":
            total, failed, bad_df = run_pattern(df, col, rsql)
        elif rtype == "RANGE":
            total, failed, bad_df = run_range(df, col, rsql, params)
        elif rtype == "DOMAIN":
            total, failed, bad_df = run_domain(df, col, rsql, params)
        elif rtype == "REF_INTEGRITY":
            total, failed, bad_df = run_ref_integrity(df, col, params)
        elif rtype == "CUSTOM_SQL":
            total, failed, bad_df = run_custom_sql(df, params)
        else:
            raise ValueError(f"Unknown rule_type: {rtype}")

        pass_rate = ((total - failed) / total) if total > 0 else 1.0
        status    = "PASS" if pass_rate >= thresh else "FAIL"

    except Exception as ex:
        total = failed = 0; pass_rate = 0.0; status = "ERROR"
        error_msg = str(ex)[:300]

    # ── Print result ─────────────────────────────────────────────────────────
    icon = "✅" if status == "PASS" else ("❌" if status == "FAIL" else "⚠️")
    print(f"  {icon} [{status:4s}] {rname:20s}  "
          f"tbl={m['table_name']:10s}  col={str(col or '-'):12s}  "
          f"pass={pass_rate:.0%}  failed={failed}")

    # ── Collect result row ────────────────────────────────────────────────────
    result_rows.append((
        RUN_ID, RUN_DATE,
        int(m["mapping_id"]), rname,
        m["table_catalog"], m["table_schema"], m["table_name"],
        col, int(total), int(failed), float(pass_rate),
        float(thresh), status, sev, error_msg
    ))

    # ── Collect failed rows (up to 500 per rule) ──────────────────────────────
    if status == "FAIL" and bad_df is not None and failed > 0:
        pk_val_col  = pk_col if pk_col in bad_df.columns else bad_df.columns[0]
        bad_val_col = col    if (col and col in bad_df.columns) else None
        rows = (bad_df
                .withColumn("_pk",  F.col(pk_val_col).cast(StringType()))
                .withColumn("_bad", F.col(bad_val_col).cast(StringType()) if bad_val_col else F.lit(None).cast(StringType()))
                .select("_pk", "_bad")
                .limit(500)
                .collect())
        for row in rows:
            failed_rows.append((RUN_ID, RUN_DATE, rname, m["table_name"], col or "",
                                row["_pk"], row["_bad"], sev))

p = sum(1 for r in result_rows if r[12] == "PASS")
f = sum(1 for r in result_rows if r[12] == "FAIL")
e = sum(1 for r in result_rows if r[12] == "ERROR")
print(f"\n{'='*65}")
print(f"  DONE: {len(result_rows)} rules  |  ✅ PASS={p}  ❌ FAIL={f}  ⚠️ ERROR={e}")

# COMMAND ----------
# MAGIC %md ## CELL 8 — Write Results to Delta Tables

# COMMAND ----------

# ── dq_run_results ────────────────────────────────────────────────────────────
res_schema = ("run_id STRING, run_date DATE, mapping_id INT, rule_name STRING,"
              " table_catalog STRING, table_schema STRING, table_name STRING,"
              " column_name STRING, total_records LONG, failed_records LONG,"
              " pass_rate DOUBLE, threshold DOUBLE, status STRING,"
              " severity STRING, error_msg STRING")

(spark.createDataFrame(result_rows, schema=res_schema)
    .write.format("delta").mode("append")
    .partitionBy("run_date", "table_catalog")
    .saveAsTable(f"{CATALOG}.results.dq_run_results"))
print(f"dq_run_results   : {len(result_rows)} rows written")

# ── dq_failed_records ────────────────────────────────────────────────────────
if failed_rows:
    fr_schema = ("run_id STRING, run_date DATE, rule_name STRING, table_name STRING,"
                 " column_name STRING, pk_value STRING, bad_value STRING, severity STRING")
    (spark.createDataFrame(failed_rows, schema=fr_schema)
        .write.format("delta").mode("append")
        .partitionBy("run_date")
        .saveAsTable(f"{CATALOG}.results.dq_failed_records"))
    print(f"dq_failed_records: {len(failed_rows)} bad rows captured")
else:
    print("dq_failed_records: nothing to write")

# ── dq_score ─────────────────────────────────────────────────────────────────
from pyspark.sql import Row
score_rows = []
by_tbl = defaultdict(list)
for r in result_rows:
    by_tbl[r[6]].append(r)   # group by table_name

for tbl, rows in by_tbl.items():
    tot  = len(rows)
    pas  = sum(1 for r in rows if r[12] == "PASS")
    fal  = sum(1 for r in rows if r[12] == "FAIL")
    err  = sum(1 for r in rows if r[12] == "ERROR")
    pct  = round(pas / tot * 100, 2) if tot > 0 else 0.0
    score_rows.append((RUN_ID, RUN_DATE, rows[0][4], rows[0][5], tbl,
                        tot, pas, fal, err, pct))

sc_schema = ("run_id STRING, run_date DATE, table_catalog STRING, table_schema STRING,"
             " table_name STRING, total_rules INT, passed INT, failed INT,"
             " errors INT, dq_score_pct DOUBLE")
(spark.createDataFrame(score_rows, schema=sc_schema)
    .write.format("delta").mode("append")
    .partitionBy("run_date")
    .saveAsTable(f"{CATALOG}.results.dq_score"))
print(f"dq_score         : {len(score_rows)} table score(s) written")

# COMMAND ----------
# MAGIC %md ## CELL 9 — View Results

# COMMAND ----------

print("=" * 65)
print("RULE RESULTS — PASS / FAIL per rule")
print("=" * 65)
spark.table(f"{CATALOG}.results.dq_run_results") \
    .filter(f"run_id = '{RUN_ID}'") \
    .select("rule_name", "table_name", "column_name",
            "total_records", "failed_records", "pass_rate", "status", "severity") \
    .orderBy("table_name", "status") \
    .show(50, truncate=False)

# COMMAND ----------

print("=" * 65)
print("DQ SCORE — Per Table")
print("=" * 65)
spark.table(f"{CATALOG}.results.dq_score") \
    .filter(f"run_id = '{RUN_ID}'") \
    .select("table_name", "total_rules", "passed",
            "failed", "errors", "dq_score_pct") \
    .show(20, truncate=False)

# COMMAND ----------

print("=" * 65)
print("FAILED RECORDS — Bad rows caught")
print("=" * 65)
spark.table(f"{CATALOG}.results.dq_failed_records") \
    .filter(f"run_id = '{RUN_ID}'") \
    .select("rule_name", "table_name", "column_name",
            "pk_value", "bad_value", "severity") \
    .orderBy("table_name", "rule_name") \
    .show(30, truncate=False)

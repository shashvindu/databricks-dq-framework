# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Data Quality Framework — Complete Test Notebook
# MAGIC ### Databricks + PySpark + Delta Lake + Unity Catalog
# MAGIC **Run all cells: Click Run → Run All**

# COMMAND ----------

# MAGIC %md ## STEP 1: Create Catalog & Schemas

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dq_framework")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.config")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.results")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.staging")
print("STEP 1 DONE ✅ Catalog and schemas created")
print("  dq_framework.config   — stores all DQ rules and config")
print("  dq_framework.results  — stores execution results and scores")
print("  dq_framework.staging  — sample source tables for testing")

# COMMAND ----------

# MAGIC %md ## STEP 2: Create All Metadata Delta Tables

# COMMAND ----------

# ── Drop & recreate for clean test run ──────────────────────────────────────
tables_to_drop = [
    "dq_framework.config.config_sources",
    "dq_framework.config.config_tables",
    "dq_framework.config.config_rules",
    "dq_framework.config.rule_mapping",
    "dq_framework.config.rule_parameters",
    "dq_framework.config.join_config",
    "dq_framework.results.execution_history",
    "dq_framework.results.failed_records",
    "dq_framework.results.audit_logs",
    "dq_framework.results.dq_score",
]
for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
print("Old tables dropped (if any)")

# ── config_sources ──────────────────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.config_sources (
    source_id    INT,
    source_name  STRING,
    catalog_name STRING,
    schema_name  STRING,
    description  STRING,
    owner        STRING,
    active       BOOLEAN,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) USING DELTA
""")

# ── config_tables ───────────────────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.config_tables (
    table_id       INT,
    source_id      INT,
    catalog_name   STRING,
    schema_name    STRING,
    table_name     STRING,
    primary_key    STRING,
    partition_cols STRING,
    active         BOOLEAN,
    created_date   TIMESTAMP,
    updated_date   TIMESTAMP
) USING DELTA
""")

# ── config_rules — master rule library ─────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.config_rules (
    rule_id           INT,
    rule_name         STRING,
    rule_type         STRING,
    rule_expression   STRING,
    default_severity  STRING,
    default_threshold DOUBLE,
    description       STRING,
    version           INT,
    active            BOOLEAN,
    created_by        STRING,
    created_date      TIMESTAMP,
    updated_by        STRING,
    updated_date      TIMESTAMP
) USING DELTA
""")

# ── rule_mapping — binds rule to table + column ─────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.rule_mapping (
    mapping_id       INT,
    rule_id          INT,
    table_id         INT,
    column_name      STRING,
    filter_condition STRING,
    join_id          INT,
    severity         STRING,
    threshold        DOUBLE,
    execution_order  INT,
    active           BOOLEAN,
    created_by       STRING,
    created_date     TIMESTAMP,
    updated_by       STRING,
    updated_date     TIMESTAMP
) USING DELTA
""")

# ── rule_parameters — key-value overrides ──────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.rule_parameters (
    param_id     INT,
    mapping_id   INT,
    param_key    STRING,
    param_value  STRING,
    created_date TIMESTAMP
) USING DELTA
""")

# ── join_config — configurable join chains ─────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.config.join_config (
    join_id        INT,
    join_name      STRING,
    left_catalog   STRING,
    left_schema    STRING,
    left_table     STRING,
    right_catalog  STRING,
    right_schema   STRING,
    right_table    STRING,
    join_type      STRING,
    join_condition STRING,
    join_order     INT,
    active         BOOLEAN,
    created_date   TIMESTAMP
) USING DELTA
""")

# ── results.execution_history ───────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.results.execution_history (
    execution_id      STRING,
    run_id            STRING,
    mapping_id        INT,
    rule_id           INT,
    rule_name         STRING,
    rule_type         STRING,
    catalog_name      STRING,
    schema_name       STRING,
    table_name        STRING,
    column_name       STRING,
    filter_condition  STRING,
    total_records     LONG,
    passed_records    LONG,
    failed_records    LONG,
    pass_rate         DOUBLE,
    threshold         DOUBLE,
    severity          STRING,
    status            STRING,
    error_message     STRING,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    duration_seconds  DOUBLE,
    framework_version STRING
) USING DELTA PARTITIONED BY (catalog_name, schema_name)
""")

# ── results.failed_records ─────────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.results.failed_records (
    failed_record_id STRING,
    execution_id     STRING,
    rule_id          INT,
    rule_name        STRING,
    catalog_name     STRING,
    schema_name      STRING,
    table_name       STRING,
    column_name      STRING,
    pk_value         STRING,
    failed_col_value STRING,
    rule_expression  STRING,
    severity         STRING,
    run_date         DATE,
    created_ts       TIMESTAMP
) USING DELTA PARTITIONED BY (run_date, catalog_name)
""")

# ── results.audit_logs ────────────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.results.audit_logs (
    log_id        STRING,
    execution_id  STRING,
    run_id        STRING,
    log_level     STRING,
    component     STRING,
    message       STRING,
    details       STRING,
    log_timestamp TIMESTAMP
) USING DELTA
""")

# ── results.dq_score ──────────────────────────────────────────────────────
spark.sql("""
CREATE TABLE dq_framework.results.dq_score (
    score_id              STRING,
    execution_id          STRING,
    run_id                STRING,
    catalog_name          STRING,
    schema_name           STRING,
    table_name            STRING,
    column_name           STRING,
    total_rules           INT,
    passed_rules          INT,
    failed_rules          INT,
    error_rules           INT,
    total_records_checked LONG,
    total_failed_records  LONG,
    dq_score              DOUBLE,
    severity_high_fails   INT,
    severity_med_fails    INT,
    severity_low_fails    INT,
    score_date            DATE,
    created_ts            TIMESTAMP
) USING DELTA PARTITIONED BY (score_date)
""")

print("STEP 2 DONE ✅ All 10 metadata Delta tables created!")
print()
for t in tables_to_drop:
    print(f"   ✓ {t}")

# COMMAND ----------

# MAGIC %md ## STEP 3: Create Sample Source Tables (Customer & Sales)

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, IntegerType,
                                StringType, DoubleType)

# ── staging.customer ────────────────────────────────────────────────────────
customer_data = [
    # Intentional BAD DATA to trigger DQ failures:
    # Row 3  : email = NULL               → NULL_CHECK fails
    # Row 4  : email = "not-an-email"     → EMAIL_REGEX fails
    # Row 4  : age = -5                   → NUMERIC_BETWEEN fails
    # Row 7  : email = ""                 → EMPTY check
    # Row 8  : age = 200, salary = -500   → RANGE + NOT_NEGATIVE fails
    # Row 8  : gender = "X"              → DOMAIN fails
    # Row 10 : duplicate customer_id=10  → UNIQUENESS fails
    # Row 11 : all nulls                 → multiple null failures
    (1,  "Alice Smith",  "alice@example.com",     30,  75000.0,  "F", "US", "ACTIVE",   "+1-555-0101"),
    (2,  "Bob Jones",    "bob.jones@corp.com",    45,  92000.0,  "M", "US", "ACTIVE",   "+1-555-0102"),
    (3,  "Carol White",  None,                    28,  60000.0,  "F", "IN", "ACTIVE",   "+91-9876543210"),
    (4,  "Dave Brown",   "not-an-email",          -5,  120000.0, "M", "UK", "INACTIVE", "123"),
    (5,  "Eve Davis",    "eve@example.com",       35,  85000.0,  "F", "US", "ACTIVE",   "+1-555-0105"),
    (6,  "Frank Lee",    "frank@example.com",     52,  200000.0, "M", "CA", "ACTIVE",   "+1-555-0106"),
    (7,  "Grace Kim",    "",                      29,  70000.0,  "F", "US", "ACTIVE",   "+1-555-0107"),
    (8,  "Harry Wilson", "harry@example.com",    200,  -500.0,   "X", "AU", "ACTIVE",   "+61-412345678"),
    (9,  "Iris Chen",    "iris@example.com",      33,  88000.0,  "F", "US", "ACTIVE",   "+1-555-0109"),
    (10, "Jack Taylor",  "jack@example.com",      41,  95000.0,  "M", "US", "ACTIVE",   "+1-555-0110"),
    (10, "Jack Taylor",  "jack@example.com",      41,  95000.0,  "M", "US", "ACTIVE",   "+1-555-0110"),  # DUPLICATE
    (11, "Karen Moore",  None,                   None, None,    None, None,  None,       None),           # ALL NULLS
]
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("email",       StringType(),  True),
    StructField("age",         IntegerType(), True),
    StructField("salary",      DoubleType(),  True),
    StructField("gender",      StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("phone",       StringType(),  True),
])
(spark.createDataFrame(customer_data, schema=customer_schema)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dq_framework.staging.customer"))

# ── staging.sales ─────────────────────────────────────────────────────────
sales_data = [
    # Row with customer_id=99 → referential integrity failure (no such customer)
    # Row with amount=None    → NULL check failure
    # Row with amount=-50     → NOT_NEGATIVE failure
    (1001, 1,  "2024-01-15",  250.0, "USD"),
    (1002, 2,  "2024-01-16",  530.0, "USD"),
    (1003, 99, "2024-01-17",  120.0, "USD"),   # orphan FK
    (1004, 5,  "2024-01-18",  890.0, "USD"),
    (1005, 6,  "2024-03-01",  None,  "USD"),   # NULL amount
    (1006, 7,  "2024-03-15",  -50.0, "USD"),   # negative amount
    (1007, 9,  "2024-04-01", 1200.0, "USD"),
    (1008, 10, "2024-04-10",  330.0, "USD"),
]
sales_schema = StructType([
    StructField("sale_id",     IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("sale_date",   StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  True),
])
(spark.createDataFrame(sales_data, schema=sales_schema)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dq_framework.staging.sales"))

print("STEP 3 DONE ✅ Sample source tables created")
print(f"  staging.customer : {spark.table('dq_framework.staging.customer').count()} rows (with intentional bad data)")
print(f"  staging.sales    : {spark.table('dq_framework.staging.sales').count()} rows (with intentional bad data)")

# COMMAND ----------

# MAGIC %md ## STEP 4: Seed Metadata Config Tables

# COMMAND ----------

from pyspark.sql import functions as F

NOW = "current_timestamp()"

# ── config_sources ──────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO dq_framework.config.config_sources VALUES
(1, 'CRM System',     'dq_framework', 'staging', 'Customer master data', 'Data Engineering', true,  {NOW}, {NOW}),
(2, 'Sales Platform', 'dq_framework', 'staging', 'Sales transactions',   'Data Engineering', true,  {NOW}, {NOW})
""")

# ── config_tables ───────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO dq_framework.config.config_tables VALUES
(1, 1, 'dq_framework', 'staging', 'customer', 'customer_id', NULL,        true, {NOW}, {NOW}),
(2, 2, 'dq_framework', 'staging', 'sales',    'sale_id',     'sale_date', true, {NOW}, {NOW})
""")

# ── config_rules — master rule library ─────────────────────────────────────
spark.sql(f"""
INSERT INTO dq_framework.config.config_rules VALUES
(1,  'NULL_CHECK',      'COMPLETENESS',  '{{col}} IS NOT NULL',                                      'HIGH',   0.95, 'Column must not be NULL',         1, true, 'system', {NOW}, NULL, {NOW}),
(2,  'EMPTY_STRING',    'COMPLETENESS',  '{{col}} IS NOT NULL AND trim({{col}}) != ""',              'MEDIUM', 0.95, 'Column must not be blank',        1, true, 'system', {NOW}, NULL, {NOW}),
(3,  'UNIQUE_CHECK',    'UNIQUENESS',    '{{col}}',                                                  'HIGH',   1.00, 'Column values must be unique',    1, true, 'system', {NOW}, NULL, {NOW}),
(5,  'EMAIL_REGEX',     'PATTERN',       '{{col}} RLIKE "^[a-zA-Z0-9._%+\\\\-]+@[a-zA-Z0-9.\\\\-]+\\\\.[a-zA-Z]{{2,}}$"', 'HIGH', 0.90, 'Email format', 1, true, 'system', {NOW}, NULL, {NOW}),
(6,  'PHONE_REGEX',     'PATTERN',       '{{col}} RLIKE "^\\\\+?[1-9][0-9]{{1,14}}$"',             'MEDIUM', 0.90, 'Phone E.164 format',              1, true, 'system', {NOW}, NULL, {NOW}),
(8,  'NUMERIC_BETWEEN', 'RANGE',         '{{col}} BETWEEN {{min_val}} AND {{max_val}}',              'HIGH',   0.95, 'Numeric range check',             1, true, 'system', {NOW}, NULL, {NOW}),
(9,  'GREATER_THAN',    'RANGE',         '{{col}} > {{min_val}}',                                   'HIGH',   0.95, 'Value must exceed threshold',     1, true, 'system', {NOW}, NULL, {NOW}),
(10, 'NOT_NEGATIVE',    'RANGE',         '{{col}} >= 0',                                             'HIGH',   0.95, 'Value must be non-negative',      1, true, 'system', {NOW}, NULL, {NOW}),
(11, 'DOMAIN_IN_LIST',  'DOMAIN',        '{{col}} IN ({{allowed_values}})',                          'HIGH',   0.95, 'Value must be in allowed list',   1, true, 'system', {NOW}, NULL, {NOW}),
(12, 'REF_INTEGRITY',   'REF_INTEGRITY', 'EXISTS_IN:{{ref_catalog}}.{{ref_schema}}.{{ref_table}}.{{ref_column}}', 'HIGH', 1.00, 'FK must exist in ref table', 1, true, 'system', {NOW}, NULL, {NOW}),
(14, 'AGG_ROW_COUNT',   'AGGREGATE',     'COUNT(*) >= {{min_count}}',                               'HIGH',   1.00, 'Table minimum row count',         1, true, 'system', {NOW}, NULL, {NOW}),
(16, 'CUSTOM_SQL',      'CUSTOM_SQL',    '{{sql_expression}}',                                      'HIGH',   0.95, 'Custom SQL boolean rule',         1, true, 'system', {NOW}, NULL, {NOW})
""")

# ── rule_mapping ────────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO dq_framework.config.rule_mapping VALUES
-- customer table rules
(1,  1,  1, 'email',       NULL, NULL, 'HIGH',   0.95, 10,  true, 'system', {NOW}, NULL, {NOW}),
(2,  5,  1, 'email',       NULL, NULL, 'HIGH',   0.90, 20,  true, 'system', {NOW}, NULL, {NOW}),
(3,  1,  1, 'name',        NULL, NULL, 'HIGH',   0.95, 30,  true, 'system', {NOW}, NULL, {NOW}),
(4,  3,  1, 'customer_id', NULL, NULL, 'HIGH',   1.00, 40,  true, 'system', {NOW}, NULL, {NOW}),
(5,  8,  1, 'age',         NULL, NULL, 'HIGH',   0.95, 50,  true, 'system', {NOW}, NULL, {NOW}),
(6,  9,  1, 'salary',      NULL, NULL, 'HIGH',   0.95, 60,  true, 'system', {NOW}, NULL, {NOW}),
(7,  11, 1, 'gender',      NULL, NULL, 'HIGH',   0.95, 70,  true, 'system', {NOW}, NULL, {NOW}),
(8,  6,  1, 'phone',       NULL, NULL, 'MEDIUM', 0.90, 80,  true, 'system', {NOW}, NULL, {NOW}),
(9,  14, 1, NULL,          NULL, NULL, 'HIGH',   1.00, 90,  true, 'system', {NOW}, NULL, {NOW}),
-- sales table rules
(10, 1,  2, 'amount',      NULL, NULL, 'HIGH',   0.95, 10,  true, 'system', {NOW}, NULL, {NOW}),
(11, 10, 2, 'amount',      NULL, NULL, 'HIGH',   0.95, 20,  true, 'system', {NOW}, NULL, {NOW}),
(12, 12, 2, 'customer_id', NULL, NULL, 'HIGH',   1.00, 30,  true, 'system', {NOW}, NULL, {NOW}),
(13, 16, 2, NULL,          NULL, NULL, 'HIGH',   0.95, 40,  true, 'system', {NOW}, NULL, {NOW})
""")

# ── rule_parameters ─────────────────────────────────────────────────────────
spark.sql(f"""
INSERT INTO dq_framework.config.rule_parameters VALUES
(1,  5,  'min_val',        '0',                                    {NOW}),
(2,  5,  'max_val',        '120',                                  {NOW}),
(3,  6,  'min_val',        '0',                                    {NOW}),
(4,  7,  'allowed_values', "'M','F','O'",                          {NOW}),
(5,  9,  'min_count',      '5',                                    {NOW}),
(6,  12, 'ref_catalog',    'dq_framework',                        {NOW}),
(7,  12, 'ref_schema',     'staging',                             {NOW}),
(8,  12, 'ref_table',      'customer',                            {NOW}),
(9,  12, 'ref_column',     'customer_id',                         {NOW}),
(10, 13, 'sql_expression', 'amount > 0 OR currency IS NOT NULL',  {NOW})
""")

print("STEP 4 DONE ✅ All metadata config tables seeded!")
print(f"  config_sources  : {spark.table('dq_framework.config.config_sources').count()} rows")
print(f"  config_tables   : {spark.table('dq_framework.config.config_tables').count()} rows")
print(f"  config_rules    : {spark.table('dq_framework.config.config_rules').count()} rules in library")
print(f"  rule_mapping    : {spark.table('dq_framework.config.rule_mapping').count()} active rule→table bindings")
print(f"  rule_parameters : {spark.table('dq_framework.config.rule_parameters').count()} parameter overrides")

# COMMAND ----------

# MAGIC %md ## STEP 5: Load All Validators & Rule Engine

# COMMAND ----------

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import uuid
from datetime import datetime, date
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# ValidationResult  — returned by every validator
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ValidationResult:
    mapping_id:     int
    rule_id:        int
    rule_name:      str
    rule_type:      str
    catalog_name:   str
    schema_name:    str
    table_name:     str
    column_name:    Optional[str]
    total_records:  int
    passed_records: int
    failed_records: int
    pass_rate:      float
    threshold:      float
    severity:       str
    status:         str
    error_message:  Optional[str] = None
    failed_df:      Optional[DataFrame] = field(default=None, repr=False)

    @classmethod
    def make_error(cls, mid, rid, rname, rtype, cat, sch, tbl, col, sev, thr, msg):
        return cls(mid, rid, rname, rtype, cat, sch, tbl, col,
                   0, 0, 0, 0.0, thr, sev, "ERROR", msg, None)

# ─────────────────────────────────────────────────────────────────────────────
# BaseValidator
# ─────────────────────────────────────────────────────────────────────────────
class BaseValidator(ABC):
    def __init__(self, spark): self._spark = spark

    @abstractmethod
    def validate(self, df, mapping, params) -> ValidationResult: ...

    def _resolve(self, template, col, params):
        expr = template
        if col: expr = expr.replace("{col}", col)
        for k, v in params.items(): expr = expr.replace(f"{{{k}}}", v)
        return expr

    def _compute(self, mapping, df, failed_df):
        total  = df.count()
        failed = failed_df.count()
        passed = total - failed
        rate   = (passed / total) if total > 0 else 1.0
        status = "PASS" if rate >= mapping["threshold"] else "FAIL"
        return ValidationResult(
            mapping["mapping_id"], mapping["rule_id"],
            mapping["rule_name"], mapping["rule_type"],
            mapping["catalog_name"], mapping["schema_name"], mapping["table_name"],
            mapping["column_name"], total, passed, failed, rate,
            mapping["threshold"], mapping["severity"], status, None, failed_df)

# ─────────────────────────────────────────────────────────────────────────────
# Concrete Validators  (Strategy Pattern)
# ─────────────────────────────────────────────────────────────────────────────

class CompletenessValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col  = mapping["column_name"]
        expr = self._resolve(mapping["rule_expression"], col, params)
        try:    return self._compute(mapping, df, df.filter(f"NOT ({expr})"))
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class UniquenessValidator(BaseValidator):
    def validate(self, df, mapping, params):
        cols = [c.strip() for c in (mapping["column_name"] or "").split(",") if c.strip()]
        try:
            w  = Window.partitionBy(*cols)
            fd = df.withColumn("_cnt", F.count("*").over(w)).filter(F.col("_cnt") > 1).drop("_cnt")
            return self._compute(mapping, df, fd)
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], mapping["column_name"],
                mapping["severity"], mapping["threshold"], str(e))

class PatternValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col  = mapping["column_name"]
        expr = self._resolve(mapping["rule_expression"], col, params)
        try:
            base   = df.filter(F.col(col).isNotNull())
            failed = base.filter(f"NOT ({expr})")
            total  = df.count(); fc = failed.count()
            rate   = ((total - fc) / total) if total > 0 else 1.0
            return ValidationResult(
                mapping["mapping_id"], mapping["rule_id"], mapping["rule_name"],
                mapping["rule_type"], mapping["catalog_name"], mapping["schema_name"],
                mapping["table_name"], col, total, total - fc, fc, rate,
                mapping["threshold"], mapping["severity"],
                "PASS" if rate >= mapping["threshold"] else "FAIL", None, failed)
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class RangeValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col  = mapping["column_name"]
        expr = self._resolve(mapping["rule_expression"], col, params)
        try:
            base = df.filter(F.col(col).isNotNull())
            return self._compute(mapping, df, base.filter(f"NOT ({expr})"))
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class DomainValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col  = mapping["column_name"]
        expr = self._resolve(mapping["rule_expression"], col, params)
        try:
            base = df.filter(F.col(col).isNotNull())
            return self._compute(mapping, df, base.filter(f"NOT ({expr})"))
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class RefIntegrityValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col = mapping["column_name"]
        try:
            ref_fq  = f"{params['ref_catalog']}.{params['ref_schema']}.{params['ref_table']}"
            ref_col = params.get("ref_column", col)
            ref_df  = self._spark.table(ref_fq).select(F.col(ref_col).alias("_ref")).distinct()
            src     = df.filter(F.col(col).isNotNull())
            failed  = src.join(ref_df, src[col] == ref_df["_ref"], "left_anti")
            return self._compute(mapping, df, failed)
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class AggregateValidator(BaseValidator):
    def validate(self, df, mapping, params):
        col  = mapping["column_name"]
        expr = self._resolve(mapping["rule_expression"], col or "*", params)
        try:
            pass_val = df.agg(F.expr(f"CASE WHEN {expr} THEN 1 ELSE 0 END").alias("p")).collect()[0]["p"]
            total    = df.count()
            failed   = 0 if pass_val == 1 else total
            rate     = 1.0 if pass_val == 1 else 0.0
            return ValidationResult(
                mapping["mapping_id"], mapping["rule_id"], mapping["rule_name"],
                mapping["rule_type"], mapping["catalog_name"], mapping["schema_name"],
                mapping["table_name"], col, total, total - failed, failed, rate,
                mapping["threshold"], mapping["severity"],
                "PASS" if rate >= mapping["threshold"] else "FAIL")
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], col,
                mapping["severity"], mapping["threshold"], str(e))

class CustomSQLValidator(BaseValidator):
    def validate(self, df, mapping, params):
        sql_expr = params.get("sql_expression", mapping["rule_expression"])
        try:    return self._compute(mapping, df, df.filter(f"NOT ({sql_expr})"))
        except Exception as e:
            return ValidationResult.make_error(mapping["mapping_id"], mapping["rule_id"],
                mapping["rule_name"], mapping["rule_type"], mapping["catalog_name"],
                mapping["schema_name"], mapping["table_name"], mapping["column_name"],
                mapping["severity"], mapping["threshold"], str(e))

# ── Validator Registry  (Factory Pattern) ────────────────────────────────────
VALIDATOR_REGISTRY = {
    "COMPLETENESS":  CompletenessValidator,
    "UNIQUENESS":    UniquenessValidator,
    "PATTERN":       PatternValidator,
    "RANGE":         RangeValidator,
    "DOMAIN":        DomainValidator,
    "REF_INTEGRITY": RefIntegrityValidator,
    "AGGREGATE":     AggregateValidator,
    "CUSTOM_SQL":    CustomSQLValidator,
}

print("STEP 5 DONE ✅ All validators loaded into memory")
print(f"  Registered rule types: {list(VALIDATOR_REGISTRY.keys())}")

# COMMAND ----------

# MAGIC %md ## STEP 6: Load Metadata from Delta Tables

# COMMAND ----------

# Load config_tables
tables = {r.table_id: r.asDict()
          for r in spark.table("dq_framework.config.config_tables")
                        .filter("active = true").collect()}

# Load config_rules
rules = {r.rule_id: r.asDict()
         for r in spark.table("dq_framework.config.config_rules")
                       .filter("active = true").collect()}

# Load rule_parameters  →  {mapping_id: {key: val}}
params_map = {}
for r in spark.table("dq_framework.config.rule_parameters").collect():
    if r.mapping_id not in params_map:
        params_map[r.mapping_id] = {}
    params_map[r.mapping_id][r.param_key] = r.param_value

# Load rule_mapping and build enriched mapping dicts
mappings = []
for r in (spark.table("dq_framework.config.rule_mapping")
               .filter("active = true")
               .orderBy("table_id", "execution_order")
               .collect()):
    rule  = rules.get(r.rule_id)
    table = tables.get(r.table_id)
    if not rule or not table:
        print(f"  WARNING: mapping_id={r.mapping_id} skipped — missing rule or table")
        continue
    mappings.append({
        "mapping_id":      r.mapping_id,
        "rule_id":         r.rule_id,
        "rule_name":       rule["rule_name"],
        "rule_type":       rule["rule_type"],
        "rule_expression": rule["rule_expression"],
        "table_id":        r.table_id,
        "catalog_name":    table["catalog_name"],
        "schema_name":     table["schema_name"],
        "table_name":      table["table_name"],
        "primary_key":     table["primary_key"],
        "column_name":     r.column_name,
        "filter_condition":r.filter_condition,
        "severity":        r.severity or "HIGH",
        "threshold":       r.threshold if r.threshold is not None else 0.95,
        "execution_order": r.execution_order or 100,
        "parameters":      params_map.get(r.mapping_id, {}),
    })

print(f"STEP 6 DONE ✅ Metadata loaded from Delta tables")
print(f"  Tables  registered : {len(tables)}")
print(f"  Rules   in library : {len(rules)}")
print(f"  Active mappings    : {len(mappings)}")

# COMMAND ----------

# MAGIC %md ## STEP 7: Run DQ Engine — Execute All Rules

# COMMAND ----------

RUN_ID      = f"run-{str(uuid.uuid4())[:8]}"
all_results = []
df_cache    = {}

print("=" * 72)
print(f"  DQ Framework Run  |  run_id = {RUN_ID}")
print("=" * 72)

by_table = defaultdict(list)
for m in mappings:
    key = f"{m['catalog_name']}.{m['schema_name']}.{m['table_name']}"
    by_table[key].append(m)

for tbl_fq, tbl_mappings in by_table.items():
    print(f"\n  Table: {tbl_fq}  ({len(tbl_mappings)} rules)")
    print("  " + "─" * 68)

    # Cache source DataFrame — read ONCE per table
    if tbl_fq not in df_cache:
        df_cache[tbl_fq] = spark.table(tbl_fq).cache()
        df_cache[tbl_fq].count()
    base_df = df_cache[tbl_fq]

    for m in sorted(tbl_mappings, key=lambda x: x["execution_order"]):
        start = datetime.utcnow()

        working_df = base_df
        if m.get("filter_condition"):
            working_df = working_df.filter(m["filter_condition"])

        ValidatorClass = VALIDATOR_REGISTRY.get(m["rule_type"])
        if not ValidatorClass:
            print(f"  [SKIP]  {m['rule_name']:28s} unknown rule_type: {m['rule_type']}")
            continue

        result = ValidatorClass(spark).validate(working_df, m, m["parameters"])
        end    = datetime.utcnow()
        dur    = (end - start).total_seconds()

        icon   = "✅ PASS" if result.status == "PASS" else ("❌ FAIL" if result.status == "FAIL" else "⚠  ERR!")
        col_lbl = result.column_name or "TABLE_LEVEL"

        print(f"  [{icon}]  {m['rule_name']:25s}  col={col_lbl:15s}"
              f"  pass={result.pass_rate:.0%}"
              f"  failed_rows={result.failed_records:>4}"
              f"  ({dur:.1f}s)")

        result._exec_id = str(uuid.uuid4())
        result._start   = start
        result._end     = end
        all_results.append(result)

p = sum(1 for r in all_results if r.status == "PASS")
f = sum(1 for r in all_results if r.status == "FAIL")
e = sum(1 for r in all_results if r.status == "ERROR")
print(f"\n{'='*72}")
print(f"  TOTAL {len(all_results)} rules  |  ✅ Passed: {p}  |  ❌ Failed: {f}  |  ⚠ Errors: {e}")
print(f"{'='*72}")

# COMMAND ----------

# MAGIC %md ## STEP 8: Write Results to Delta Tables

# COMMAND ----------

FRAMEWORK_VER = "1.0.0"

# ── execution_history ────────────────────────────────────────────────────────
exec_rows = []
for r in all_results:
    exec_rows.append((
        r._exec_id, RUN_ID, r.mapping_id, r.rule_id,
        r.rule_name, r.rule_type, r.catalog_name, r.schema_name,
        r.table_name, r.column_name, None,
        int(r.total_records), int(r.passed_records), int(r.failed_records),
        float(r.pass_rate), float(r.threshold),
        r.severity, r.status, r.error_message,
        r._start, r._end,
        float((r._end - r._start).total_seconds()),
        FRAMEWORK_VER
    ))

exec_schema = ("execution_id STRING, run_id STRING, mapping_id INT, rule_id INT,"
               " rule_name STRING, rule_type STRING, catalog_name STRING, schema_name STRING,"
               " table_name STRING, column_name STRING, filter_condition STRING,"
               " total_records LONG, passed_records LONG, failed_records LONG,"
               " pass_rate DOUBLE, threshold DOUBLE, severity STRING, status STRING,"
               " error_message STRING, start_time TIMESTAMP, end_time TIMESTAMP,"
               " duration_seconds DOUBLE, framework_version STRING")

(spark.createDataFrame(exec_rows, schema=exec_schema)
    .write.format("delta").mode("append")
    .saveAsTable("dq_framework.results.execution_history"))
print(f"  execution_history : {len(exec_rows)} rows written ✅")

# ── failed_records ───────────────────────────────────────────────────────────
fr_rows = []
for r in all_results:
    if r.status == "FAIL" and r.failed_df is not None and r.failed_records > 0:
        pk = (r.column_name or "customer_id")
        fd = r.failed_df
        pk_col = pk if pk in fd.columns else fd.columns[0]
        fd = fd.withColumn("_pk",  F.col(pk_col).cast(StringType()))
        if r.column_name and r.column_name in fd.columns:
            fd = fd.withColumn("_fv", F.col(r.column_name).cast(StringType()))
        else:
            fd = fd.withColumn("_fv", F.lit(None).cast(StringType()))

        for row in fd.select("_pk", "_fv").limit(200).collect():
            fr_rows.append((
                f"{r._exec_id}_{uuid.uuid4().hex[:6]}",
                r._exec_id, r.rule_id, r.rule_name,
                r.catalog_name, r.schema_name, r.table_name, r.column_name,
                row["_pk"], row["_fv"], r.rule_name, r.severity,
                date.today(), datetime.utcnow()
            ))

if fr_rows:
    fr_schema = ("failed_record_id STRING, execution_id STRING, rule_id INT, rule_name STRING,"
                 " catalog_name STRING, schema_name STRING, table_name STRING, column_name STRING,"
                 " pk_value STRING, failed_col_value STRING, rule_expression STRING,"
                 " severity STRING, run_date DATE, created_ts TIMESTAMP")
    (spark.createDataFrame(fr_rows, schema=fr_schema)
        .write.format("delta").mode("append")
        .partitionBy("run_date", "catalog_name")
        .saveAsTable("dq_framework.results.failed_records"))
    print(f"  failed_records    : {len(fr_rows)} failed rows written ✅")
else:
    print("  failed_records    : 0 rows (no FAIL results)")

# ── dq_score ─────────────────────────────────────────────────────────────────
score_rows = []
by_tbl = defaultdict(list)
for r in all_results:
    by_tbl[r.table_name].append(r)

for tbl_name, tbl_results in by_tbl.items():
    cat  = tbl_results[0].catalog_name
    sch  = tbl_results[0].schema_name
    tot  = len(tbl_results)
    pas  = sum(1 for r in tbl_results if r.status == "PASS")
    fal  = sum(1 for r in tbl_results if r.status == "FAIL")
    err  = sum(1 for r in tbl_results if r.status == "ERROR")
    trec = sum(r.total_records  for r in tbl_results if r.total_records)
    tfai = sum(r.failed_records for r in tbl_results if r.failed_records)
    dqs  = round((pas / tot * 100.0) if tot > 0 else 0.0, 2)
    hf   = sum(1 for r in tbl_results if r.status == "FAIL" and r.severity == "HIGH")
    score_rows.append((
        str(uuid.uuid4()), all_results[0]._exec_id, RUN_ID,
        cat, sch, tbl_name, None,
        tot, pas, fal, err, int(trec), int(tfai), dqs, hf, 0, 0,
        date.today(), datetime.utcnow()
    ))

score_schema = ("score_id STRING, execution_id STRING, run_id STRING,"
                " catalog_name STRING, schema_name STRING, table_name STRING, column_name STRING,"
                " total_rules INT, passed_rules INT, failed_rules INT, error_rules INT,"
                " total_records_checked LONG, total_failed_records LONG, dq_score DOUBLE,"
                " severity_high_fails INT, severity_med_fails INT, severity_low_fails INT,"
                " score_date DATE, created_ts TIMESTAMP")
(spark.createDataFrame(score_rows, schema=score_schema)
    .write.format("delta").mode("append")
    .partitionBy("score_date")
    .saveAsTable("dq_framework.results.dq_score"))
print(f"  dq_score          : {len(score_rows)} table score(s) written ✅")
print("\nSTEP 8 DONE ✅ All results persisted to Delta tables!")

# COMMAND ----------

# MAGIC %md ## STEP 9: Results Dashboard

# COMMAND ----------

print("=" * 72)
print("  EXECUTION HISTORY — Rule-by-Rule Results")
print("=" * 72)
spark.table("dq_framework.results.execution_history") \
    .filter(f"run_id = '{RUN_ID}'") \
    .select("rule_name", "rule_type", "table_name", "column_name",
            "total_records", "failed_records", "pass_rate", "severity", "status") \
    .orderBy("table_name", "status") \
    .show(50, truncate=False)

# COMMAND ----------

print("=" * 72)
print("  DQ SCORE SUMMARY — Per Table")
print("=" * 72)
spark.table("dq_framework.results.dq_score") \
    .filter(f"run_id = '{RUN_ID}'") \
    .select("table_name", "total_rules", "passed_rules", "failed_rules",
            "total_records_checked", "total_failed_records", "dq_score") \
    .show(20, truncate=False)

# COMMAND ----------

print("=" * 72)
print("  FAILED RECORDS — What exactly failed and why")
print("=" * 72)
spark.table("dq_framework.results.failed_records") \
    .filter(f"execution_id = '{all_results[0]._exec_id}'") \
    .select("rule_name", "table_name", "column_name",
            "pk_value", "failed_col_value", "severity") \
    .orderBy("table_name", "rule_name") \
    .show(30, truncate=False)

# COMMAND ----------

# MAGIC %md ## STEP 10: Unit Tests — All Validators

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

passed_tests, failed_tests = [], []

def assert_result(name, result, expected):
    if result.status == expected:
        print(f"  ✅ PASS  {name}")
        passed_tests.append(name)
    else:
        print(f"  ❌ FAIL  {name}  expected={expected} got={result.status}  "
              f"pass_rate={result.pass_rate:.1%}  err={result.error_message}")
        failed_tests.append(name)

def make_mapping(rule_type, rule_expression, col=None, threshold=0.95, params=None):
    return {"mapping_id": 999, "rule_id": 1, "rule_name": rule_type, "rule_type": rule_type,
            "rule_expression": rule_expression, "table_id": 1,
            "catalog_name": "test", "schema_name": "test", "table_name": "test_tbl",
            "column_name": col, "filter_condition": None, "primary_key": "id",
            "severity": "HIGH", "threshold": threshold, "execution_order": 1,
            "parameters": params or {}}

print("Running Unit Tests...")
print("─" * 60)

# TEST 1: Completeness
df1 = spark.createDataFrame([(1,"a@b.com"),(2,None),(3,"c@d.com")],
      StructType([StructField("id",IntegerType()),StructField("email",StringType())]))
m1  = make_mapping("COMPLETENESS", "{col} IS NOT NULL", "email", 0.90)
assert_result("NULL_CHECK → FAIL (1/3 null @ threshold 90%)",
              CompletenessValidator(spark).validate(df1, m1, {}), "FAIL")

m1b = make_mapping("COMPLETENESS", "{col} IS NOT NULL", "email", 0.50)
assert_result("NULL_CHECK → PASS (66% pass rate > 50% threshold)",
              CompletenessValidator(spark).validate(df1, m1b, {}), "PASS")

# TEST 2: Uniqueness
df2 = spark.createDataFrame([(1,"A"),(2,"B"),(2,"B")],
      StructType([StructField("id",IntegerType()),StructField("name",StringType())]))
m2  = make_mapping("UNIQUENESS", "{col}", "id", 1.00)
assert_result("UNIQUE_CHECK → FAIL (id=2 is duplicate)",
              UniquenessValidator(spark).validate(df2, m2, {}), "FAIL")

# TEST 3: Pattern — Email regex
df3 = spark.createDataFrame([(1,"valid@example.com"),(2,"not-an-email"),(3,"bad"),(4,"ok@x.org"),(5,None)],
      StructType([StructField("id",IntegerType()),StructField("email",StringType())]))
m3  = make_mapping("PATTERN",
      r'{col} RLIKE "^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"', "email", 0.90)
assert_result("EMAIL_REGEX → FAIL (2 of 4 non-null emails are bad)",
              PatternValidator(spark).validate(df3, m3, {}), "FAIL")

# TEST 4: Range — BETWEEN
df4 = spark.createDataFrame([(1,25),(2,-1),(3,150),(4,45),(5,None)],
      StructType([StructField("id",IntegerType()),StructField("age",IntegerType())]))
m4  = make_mapping("RANGE", "{col} BETWEEN {min_val} AND {max_val}", "age", 0.80,
                   {"min_val": "0", "max_val": "120"})
assert_result("NUMERIC_BETWEEN 0-120 → FAIL (age=-1, age=150 out of range)",
              RangeValidator(spark).validate(df4, m4, m4["parameters"]), "FAIL")

# TEST 5: Domain
df5 = spark.createDataFrame([(1,"M"),(2,"F"),(3,"X"),(4,"O"),(5,None)],
      StructType([StructField("id",IntegerType()),StructField("gender",StringType())]))
m5  = make_mapping("DOMAIN", "{col} IN ({allowed_values})", "gender", 0.95,
                   {"allowed_values": "'M','F','O'"})
assert_result("DOMAIN IN (M,F,O) → FAIL (X is invalid)",
              DomainValidator(spark).validate(df5, m5, m5["parameters"]), "FAIL")

# TEST 6: Referential Integrity (anti-join verification)
ref_df = spark.createDataFrame([(1,),(2,),(3,),(4,),(5,)], ["customer_id"])
fk_df  = spark.createDataFrame([(1001,1),(1002,2),(1003,99),(1004,5)],
          StructType([StructField("sale_id",IntegerType()),StructField("customer_id",IntegerType())]))
src    = fk_df.filter(F.col("customer_id").isNotNull())
orphan_count = src.join(ref_df, src["customer_id"] == ref_df["customer_id"], "left_anti").count()
if orphan_count == 1:
    print("  ✅ PASS  REF_INTEGRITY anti-join finds 1 orphan FK (customer_id=99)")
    passed_tests.append("REF_INTEGRITY")
else:
    print(f"  ❌ FAIL  REF_INTEGRITY expected 1 orphan, got {orphan_count}")
    failed_tests.append("REF_INTEGRITY")

# TEST 7: Aggregate
df7 = spark.createDataFrame([(i,) for i in range(1, 11)],
      StructType([StructField("id",IntegerType())]))
m7a = make_mapping("AGGREGATE","COUNT(*) >= {min_count}", None, 1.00, {"min_count":"5"})
assert_result("AGG COUNT>=5 → PASS (10 rows)",
              AggregateValidator(spark).validate(df7, m7a, m7a["parameters"]), "PASS")

m7b = make_mapping("AGGREGATE","COUNT(*) >= {min_count}", None, 1.00, {"min_count":"100"})
assert_result("AGG COUNT>=100 → FAIL (only 10 rows)",
              AggregateValidator(spark).validate(df7, m7b, m7b["parameters"]), "FAIL")

# TEST 8: Custom SQL
df8 = spark.createDataFrame([(1,100.0),(2,-50.0),(3,None),(4,200.0)],
      StructType([StructField("id",IntegerType()),StructField("amount",DoubleType())]))
m8  = make_mapping("CUSTOM_SQL","{sql_expression}", None, 0.90,
                   {"sql_expression":"amount > 0 OR amount IS NULL"})
assert_result("CUSTOM_SQL → FAIL (-50 violates amount > 0)",
              CustomSQLValidator(spark).validate(df8, m8, m8["parameters"]), "FAIL")

# Summary
print("─" * 60)
total = len(passed_tests) + len(failed_tests)
print(f"UNIT TEST SUMMARY:  {len(passed_tests)}/{total} passed")
if failed_tests:
    print("FAILED TESTS:")
    for t in failed_tests: print(f"   ✗ {t}")
else:
    print("ALL UNIT TESTS PASSED! ✅")

# COMMAND ----------

# MAGIC %md ## STEP 11: Verify All Delta Tables in Unity Catalog

# COMMAND ----------

print("Verifying all Delta tables in Unity Catalog...")
print("─" * 60)
tables_check = [
    "dq_framework.config.config_sources",
    "dq_framework.config.config_tables",
    "dq_framework.config.config_rules",
    "dq_framework.config.rule_mapping",
    "dq_framework.config.rule_parameters",
    "dq_framework.config.join_config",
    "dq_framework.results.execution_history",
    "dq_framework.results.failed_records",
    "dq_framework.results.audit_logs",
    "dq_framework.results.dq_score",
    "dq_framework.staging.customer",
    "dq_framework.staging.sales",
]
all_ok = True
for t in tables_check:
    try:
        cnt = spark.table(t).count()
        print(f"  ✅  {t:<55s}  rows={cnt}")
    except Exception as ex:
        print(f"  ❌  {t:<55s}  ERROR: {ex}")
        all_ok = False

print()
if all_ok:
    print("✅ All tables verified successfully!")
else:
    print("⚠  Some tables had errors — check above")

print()
print("=" * 72)
print("  DQ Framework Complete Test Run DONE!")
print(f"  Results stored under run_id = {RUN_ID}")
print("  Check: dq_framework.results.* tables for full details")
print("=" * 72)

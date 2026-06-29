"""
================================================================================
Enterprise Data Quality Framework
Step 1 – Create All Metadata Delta Tables
================================================================================
Run this notebook ONCE to bootstrap the framework inside Unity Catalog.
All tables land in the  dq_framework  catalog.

Schemas created
    dq_framework.config   – framework configuration tables
    dq_framework.results  – execution results, audit logs, DQ scores

Usage (Databricks notebook cell or spark-submit)
    %run ./00_setup/01_create_metadata_tables
================================================================================
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# 0.  Create catalog + schemas
# ============================================================================

spark.sql("CREATE CATALOG IF NOT EXISTS dq_framework")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.config")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.results")
spark.sql("CREATE SCHEMA IF NOT EXISTS dq_framework.staging")   # test data

print("✅ Catalog and schemas created.")

# ============================================================================
# 1.  config.config_sources
#     One row per source system / data domain
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.config_sources (
    source_id       INT           NOT NULL COMMENT 'Unique source identifier',
    source_name     STRING        NOT NULL COMMENT 'Logical source system name',
    catalog_name    STRING        NOT NULL COMMENT 'Unity Catalog catalog',
    schema_name     STRING        NOT NULL COMMENT 'Unity Catalog schema',
    description     STRING        COMMENT 'Free-text description',
    owner           STRING        COMMENT 'Responsible team / person',
    active          BOOLEAN       NOT NULL DEFAULT true,
    created_date    TIMESTAMP     DEFAULT current_timestamp(),
    updated_date    TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Registered source systems for the DQ framework'
""")

# ============================================================================
# 2.  config.config_tables
#     One row per table registered for DQ checks
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.config_tables (
    table_id        INT           NOT NULL COMMENT 'Unique table identifier',
    source_id       INT           NOT NULL COMMENT 'FK -> config_sources.source_id',
    catalog_name    STRING        NOT NULL,
    schema_name     STRING        NOT NULL,
    table_name      STRING        NOT NULL,
    primary_key     STRING        COMMENT 'Comma-separated PK columns',
    partition_cols  STRING        COMMENT 'Comma-separated partition columns',
    active          BOOLEAN       NOT NULL DEFAULT true,
    created_date    TIMESTAMP     DEFAULT current_timestamp(),
    updated_date    TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Tables registered for Data Quality validation'
""")

# ============================================================================
# 3.  config.config_columns
#     Column-level metadata (data type hints, nullable flag, etc.)
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.config_columns (
    column_id       INT           NOT NULL,
    table_id        INT           NOT NULL COMMENT 'FK -> config_tables.table_id',
    column_name     STRING        NOT NULL,
    data_type       STRING        COMMENT 'e.g. STRING, INT, DATE, DOUBLE',
    is_nullable     BOOLEAN       DEFAULT true,
    is_pii          BOOLEAN       DEFAULT false COMMENT 'PII masking flag',
    active          BOOLEAN       NOT NULL DEFAULT true,
    created_date    TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Column-level metadata for registered tables'
""")

# ============================================================================
# 4.  config.config_rules
#     Master rule definitions – no table/column binding here
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.config_rules (
    rule_id             INT           NOT NULL COMMENT 'Unique rule identifier',
    rule_name           STRING        NOT NULL COMMENT 'Human-readable name',
    rule_type           STRING        NOT NULL COMMENT 'COMPLETENESS|UNIQUENESS|PATTERN|RANGE|DOMAIN|REF_INTEGRITY|CROSS_COLUMN|CROSS_TABLE|AGGREGATE|CUSTOM_SQL',
    rule_expression     STRING        NOT NULL COMMENT 'Dynamic expression / SQL fragment',
    default_severity    STRING        NOT NULL DEFAULT 'HIGH' COMMENT 'HIGH|MEDIUM|LOW',
    default_threshold   DOUBLE        DEFAULT 0.95 COMMENT 'Min pass rate (0.0–1.0)',
    description         STRING,
    version             INT           NOT NULL DEFAULT 1,
    active              BOOLEAN       NOT NULL DEFAULT true,
    created_by          STRING        DEFAULT 'system',
    created_date        TIMESTAMP     DEFAULT current_timestamp(),
    updated_by          STRING,
    updated_date        TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Master catalog of reusable DQ rules'
""")

# ============================================================================
# 5.  config.rule_mapping
#     Binds a rule to a specific table+column with overrides
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.rule_mapping (
    mapping_id          INT           NOT NULL,
    rule_id             INT           NOT NULL COMMENT 'FK -> config_rules.rule_id',
    table_id            INT           NOT NULL COMMENT 'FK -> config_tables.table_id',
    column_name         STRING        COMMENT 'NULL means table-level rule',
    filter_condition    STRING        COMMENT 'Optional WHERE clause e.g. country=US',
    join_id             INT           COMMENT 'FK -> join_config.join_id (optional)',
    severity            STRING        COMMENT 'Override default_severity if set',
    threshold           DOUBLE        COMMENT 'Override default_threshold if set',
    execution_order     INT           DEFAULT 100,
    active              BOOLEAN       NOT NULL DEFAULT true,
    created_by          STRING        DEFAULT 'system',
    created_date        TIMESTAMP     DEFAULT current_timestamp(),
    updated_by          STRING,
    updated_date        TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Maps DQ rules to specific tables and columns'
""")

# ============================================================================
# 6.  config.rule_parameters
#     Key-value parameters that override placeholders in rule_expression
#     e.g.  rule_expression = "col BETWEEN {min_val} AND {max_val}"
#            parameters      = {min_val: 0, max_val: 150}
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.rule_parameters (
    param_id        INT           NOT NULL,
    mapping_id      INT           NOT NULL COMMENT 'FK -> rule_mapping.mapping_id',
    param_key       STRING        NOT NULL,
    param_value     STRING        NOT NULL,
    created_date    TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Key-value parameter overrides per rule-mapping'
""")

# ============================================================================
# 7.  config.join_config
#     Configurable multi-table join chains
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.config.join_config (
    join_id             INT           NOT NULL,
    join_name           STRING        NOT NULL,
    left_catalog        STRING        NOT NULL,
    left_schema         STRING        NOT NULL,
    left_table          STRING        NOT NULL,
    right_catalog       STRING        NOT NULL,
    right_schema        STRING        NOT NULL,
    right_table         STRING        NOT NULL,
    join_type           STRING        NOT NULL COMMENT 'INNER|LEFT|RIGHT|FULL',
    join_condition      STRING        NOT NULL COMMENT 'SQL ON clause',
    join_order          INT           DEFAULT 1 COMMENT 'Sequence for multi-hop joins',
    active              BOOLEAN       NOT NULL DEFAULT true,
    created_date        TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Configurable join chains for cross-table DQ rules'
""")

# ============================================================================
# 8.  results.execution_history
#     One row per rule execution
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.results.execution_history (
    execution_id        STRING        NOT NULL COMMENT 'UUID',
    run_id              STRING        COMMENT 'Databricks job run_id',
    mapping_id          INT           NOT NULL,
    rule_id             INT           NOT NULL,
    rule_name           STRING,
    rule_type           STRING,
    catalog_name        STRING,
    schema_name         STRING,
    table_name          STRING,
    column_name         STRING,
    filter_condition    STRING,
    total_records       LONG,
    passed_records      LONG,
    failed_records      LONG,
    pass_rate           DOUBLE,
    threshold           DOUBLE,
    severity            STRING,
    status              STRING        COMMENT 'PASS|FAIL|ERROR|SKIPPED',
    error_message       STRING,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    DOUBLE,
    framework_version   STRING
)
USING DELTA
PARTITIONED BY (catalog_name, schema_name)
COMMENT 'Execution results for every DQ rule run'
""")

# ============================================================================
# 9.  results.failed_records
#     Stores the actual rows that failed a DQ rule
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.results.failed_records (
    failed_record_id    STRING        NOT NULL COMMENT 'UUID',
    execution_id        STRING        NOT NULL COMMENT 'FK -> execution_history',
    rule_id             INT,
    rule_name           STRING,
    catalog_name        STRING,
    schema_name         STRING,
    table_name          STRING,
    column_name         STRING,
    primary_key_value   STRING        COMMENT 'JSON string of PK columns+values',
    failed_column_value STRING        COMMENT 'Actual value that failed',
    rule_expression     STRING,
    severity            STRING,
    run_date            DATE          DEFAULT current_date(),
    created_timestamp   TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (run_date, catalog_name)
COMMENT 'Individual records that failed a DQ rule'
""")

# ============================================================================
# 10.  results.audit_logs
#      Framework-level operational audit trail
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.results.audit_logs (
    log_id              STRING        NOT NULL COMMENT 'UUID',
    execution_id        STRING,
    run_id              STRING,
    log_level           STRING        COMMENT 'INFO|WARNING|ERROR|DEBUG',
    component           STRING        COMMENT 'Which framework component logged this',
    message             STRING,
    details             STRING        COMMENT 'JSON for extra context',
    log_timestamp       TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Operational audit trail for the DQ framework'
""")

# ============================================================================
# 11.  results.dq_score
#      Aggregated DQ score per table (and optionally column) per run
# ============================================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_framework.results.dq_score (
    score_id            STRING        NOT NULL COMMENT 'UUID',
    execution_id        STRING        NOT NULL COMMENT 'FK -> execution_history',
    run_id              STRING,
    catalog_name        STRING,
    schema_name         STRING,
    table_name          STRING,
    column_name         STRING        COMMENT 'NULL = table-level score',
    total_rules         INT,
    passed_rules        INT,
    failed_rules        INT,
    error_rules         INT,
    total_records_checked LONG,
    total_failed_records  LONG,
    dq_score            DOUBLE        COMMENT 'passed_rules / total_rules * 100',
    severity_high_fails INT           DEFAULT 0,
    severity_med_fails  INT           DEFAULT 0,
    severity_low_fails  INT           DEFAULT 0,
    score_date          DATE          DEFAULT current_date(),
    created_timestamp   TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (score_date)
COMMENT 'Aggregated DQ scores per table per run'
""")

print("✅ All 11 metadata Delta tables created successfully.")
print()
print("Tables created:")
for t in [
    "dq_framework.config.config_sources",
    "dq_framework.config.config_tables",
    "dq_framework.config.config_columns",
    "dq_framework.config.config_rules",
    "dq_framework.config.rule_mapping",
    "dq_framework.config.rule_parameters",
    "dq_framework.config.join_config",
    "dq_framework.results.execution_history",
    "dq_framework.results.failed_records",
    "dq_framework.results.audit_logs",
    "dq_framework.results.dq_score",
]:
    print(f"   ✓ {t}")

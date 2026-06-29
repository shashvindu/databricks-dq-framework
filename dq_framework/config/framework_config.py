"""
================================================================================
Enterprise Data Quality Framework – Framework Configuration
================================================================================
All catalog / schema / table names are defined here.
Change only this file to point the framework at a different Unity Catalog
catalog or schema.
================================================================================
"""

# ---------------------------------------------------------------------------
# Unity Catalog – Metadata Layer
# ---------------------------------------------------------------------------
DQ_CATALOG  = "dq_framework"       # Unity Catalog catalog name
DQ_SCHEMA   = "config"             # Schema that holds all metadata tables

# ---------------------------------------------------------------------------
# Fully-qualified metadata table names
# ---------------------------------------------------------------------------
TBL_CONFIG_SOURCES      = f"{DQ_CATALOG}.config.config_sources"
TBL_CONFIG_TABLES       = f"{DQ_CATALOG}.config.config_tables"
TBL_CONFIG_COLUMNS      = f"{DQ_CATALOG}.config.config_columns"
TBL_CONFIG_RULES        = f"{DQ_CATALOG}.config.config_rules"
TBL_RULE_MAPPING        = f"{DQ_CATALOG}.config.rule_mapping"
TBL_RULE_PARAMETERS     = f"{DQ_CATALOG}.config.rule_parameters"
TBL_JOIN_CONFIG         = f"{DQ_CATALOG}.config.join_config"

# Result / Audit tables (separate schema)
DQ_RESULTS_SCHEMA       = "results"
TBL_EXECUTION_HISTORY   = f"{DQ_CATALOG}.results.execution_history"
TBL_FAILED_RECORDS      = f"{DQ_CATALOG}.results.failed_records"
TBL_AUDIT_LOGS          = f"{DQ_CATALOG}.results.audit_logs"
TBL_DQ_SCORE            = f"{DQ_CATALOG}.results.dq_score"

# ---------------------------------------------------------------------------
# Execution settings
# ---------------------------------------------------------------------------
BATCH_SIZE              = 50        # Max rules processed per batch
DEFAULT_SEVERITY        = "HIGH"    # Severity applied when not specified
DEFAULT_THRESHOLD       = 0.95      # Min pass-rate before a rule is flagged
CACHE_SOURCE_TABLES     = True      # Cache source DataFrames in Spark memory
PARALLEL_TABLE_EXEC     = False     # True → use ThreadPoolExecutor per table

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL               = "INFO"    # DEBUG | INFO | WARNING | ERROR
FRAMEWORK_VERSION       = "1.0.0"

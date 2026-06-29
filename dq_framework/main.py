"""
================================================================================
Enterprise Data Quality Framework
main.py  –  End-to-End Entry Point
================================================================================
Run this file on Databricks (notebook or job) to execute the full DQ pipeline.

Sequence
--------
1. Bootstrap metadata tables + seed sample data (first-run only)
2. Run MetadataLoader → loads all config from Delta tables
3. Run DQOrchestrator → executes every active rule
4. AlertManager → fires alerts on HIGH severity failures
5. Print result summary

How to run on Databricks
------------------------
Option A – Notebook cell:
    %run /path/to/dq_framework/main

Option B – Databricks Job:
    Entry point: dq_framework/main.py

Option C – Databricks CLI / spark-submit:
    spark-submit --deploy-mode client main.py
================================================================================
"""

import sys, os, uuid

# Make all sub-packages importable regardless of working directory
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyspark.sql import SparkSession

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Get or create SparkSession
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Enterprise-DQ-Framework")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("Spark version :", spark.version)

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Bootstrap (run once – idempotent CREATE IF NOT EXISTS)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("STEP 1 — Bootstrapping metadata tables …")
print("="*70)

exec(open(os.path.join(_ROOT, "00_setup", "01_create_metadata_tables.py")).read())

# ─────────────────────────────────────────────────────────────────────────────
# 3.  Seed sample data (run once – or re-seed to reset test data)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("STEP 2 — Seeding sample data …")
print("="*70)

exec(open(os.path.join(_ROOT, "00_setup", "02_seed_sample_data.py")).read())

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Run the DQ Orchestrator
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("STEP 3 — Running DQ Orchestrator …")
print("="*70)

from orchestrator.dq_orchestrator import DQOrchestrator
from alerts.alert_manager         import AlertManager

RUN_ID = f"run-{str(uuid.uuid4())[:8]}"

orchestrator = DQOrchestrator(spark, run_id=RUN_ID)
results      = orchestrator.run()

# ─────────────────────────────────────────────────────────────────────────────
# 5.  Fire alerts
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("STEP 4 — Evaluating alerts …")
print("="*70)

alert_mgr = AlertManager(spark, alert_severity="HIGH")
alert_mgr.evaluate_and_alert(results, run_id=RUN_ID)

# ─────────────────────────────────────────────────────────────────────────────
# 6.  Display results in notebook (if running in Databricks notebook)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("STEP 5 — Results Preview")
print("="*70)

print("\n--- Execution History (last run) ---")
spark.table("dq_framework.results.execution_history") \
    .orderBy("table_name", "rule_name") \
    .select("rule_name", "rule_type", "table_name", "column_name",
            "total_records", "passed_records", "failed_records",
            "pass_rate", "threshold", "severity", "status") \
    .show(50, truncate=False)

print("\n--- DQ Scores ---")
spark.table("dq_framework.results.dq_score") \
    .filter("column_name IS NULL") \
    .orderBy("table_name") \
    .select("table_name", "total_rules", "passed_rules", "failed_rules",
            "total_records_checked", "total_failed_records", "dq_score") \
    .show(20, truncate=False)

print("\n--- Failed Records Sample ---")
spark.table("dq_framework.results.failed_records") \
    .select("rule_name", "table_name", "column_name",
            "primary_key_value", "failed_column_value", "severity") \
    .show(20, truncate=False)

print("\nDQ Framework run complete ✅")

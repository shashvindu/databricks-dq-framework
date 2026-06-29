"""
================================================================================
Enterprise Data Quality Framework
Component: DQScoreEngine
================================================================================
Aggregates per-rule ValidationResults into a per-table (and per-column)
DQ Score and writes it to  dq_framework.results.dq_score.
================================================================================
"""

from __future__ import annotations
import uuid
from datetime import date, datetime
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, DateType, TimestampType
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.framework_config import TBL_DQ_SCORE
from validators.base_validator import ValidationResult


class DQScoreEngine:
    """
    Computes DQ scores from a list of ValidationResults and persists them.

    DQ Score formula (per table)
    ----------------------------
    score = (rules_passed / total_active_rules) * 100

    Usage
    -----
    scorer = DQScoreEngine(spark, run_id="job-123")
    scorer.compute_and_save(results, execution_id="uuid-abc")
    """

    def __init__(self, spark: SparkSession, run_id: str):
        self._spark  = spark
        self._run_id = run_id

    def compute_and_save(
        self,
        results:      List[ValidationResult],
        execution_id: str,
    ):
        if not results:
            print("[DQScoreEngine] No results to score.")
            return

        # Group by (catalog, schema, table)
        table_groups: dict = {}
        for r in results:
            key = (r.catalog_name, r.schema_name, r.table_name)
            if key not in table_groups:
                table_groups[key] = []
            table_groups[key].append(r)

        score_rows = []
        for (cat, sch, tbl), group_results in table_groups.items():
            row = self._build_score_row(
                execution_id, cat, sch, tbl, None, group_results
            )
            score_rows.append(row)
            # Optionally also compute column-level scores
            col_groups: dict = {}
            for r in group_results:
                if r.column_name:
                    if r.column_name not in col_groups:
                        col_groups[r.column_name] = []
                    col_groups[r.column_name].append(r)
            for col_name, col_results in col_groups.items():
                crow = self._build_score_row(
                    execution_id, cat, sch, tbl, col_name, col_results
                )
                score_rows.append(crow)

        schema = StructType([
            StructField("score_id",               StringType(),    False),
            StructField("execution_id",           StringType(),    False),
            StructField("run_id",                 StringType(),    True),
            StructField("catalog_name",           StringType(),    True),
            StructField("schema_name",            StringType(),    True),
            StructField("table_name",             StringType(),    True),
            StructField("column_name",            StringType(),    True),
            StructField("total_rules",            IntegerType(),   True),
            StructField("passed_rules",           IntegerType(),   True),
            StructField("failed_rules",           IntegerType(),   True),
            StructField("error_rules",            IntegerType(),   True),
            StructField("total_records_checked",  LongType(),      True),
            StructField("total_failed_records",   LongType(),      True),
            StructField("dq_score",              DoubleType(),    True),
            StructField("severity_high_fails",    IntegerType(),   True),
            StructField("severity_med_fails",     IntegerType(),   True),
            StructField("severity_low_fails",     IntegerType(),   True),
            StructField("score_date",             DateType(),      True),
            StructField("created_timestamp",      TimestampType(), True),
        ])

        df = self._spark.createDataFrame(score_rows, schema=schema)
        df.write.format("delta").mode("append") \
            .partitionBy("score_date") \
            .saveAsTable(TBL_DQ_SCORE)

        print(f"\n[DQScoreEngine] DQ Scores written for {len(table_groups)} table(s):")
        for row in [r for r in score_rows if r[6] is None]:   # table-level rows
            print(f"  {row[3]}.{row[4]}.{row[5]:20s}  "
                  f"Score: {row[13]:.1f}%  "
                  f"Rules: {row[7]} total | {row[8]} passed | {row[9]} failed")

    # ------------------------------------------------------------------ #
    #  Helper                                                              #
    # ------------------------------------------------------------------ #

    def _build_score_row(
        self, execution_id, cat, sch, tbl, col, results: List[ValidationResult]
    ):
        total   = len(results)
        passed  = sum(1 for r in results if r.status == "PASS")
        failed  = sum(1 for r in results if r.status == "FAIL")
        errors  = sum(1 for r in results if r.status == "ERROR")
        total_rec = sum(r.total_records  for r in results if r.total_records)
        total_fail = sum(r.failed_records for r in results if r.failed_records)
        dq_score  = (passed / total * 100.0) if total > 0 else 0.0
        high_f    = sum(1 for r in results if r.status == "FAIL" and r.severity == "HIGH")
        med_f     = sum(1 for r in results if r.status == "FAIL" and r.severity == "MEDIUM")
        low_f     = sum(1 for r in results if r.status == "FAIL" and r.severity == "LOW")

        return (
            str(uuid.uuid4()),      # score_id
            execution_id,           # execution_id
            self._run_id,           # run_id
            cat,                    # catalog_name
            sch,                    # schema_name
            tbl,                    # table_name
            col,                    # column_name (None = table-level)
            total,                  # total_rules
            passed,                 # passed_rules
            failed,                 # failed_rules
            errors,                 # error_rules
            total_rec,              # total_records_checked
            total_fail,             # total_failed_records
            dq_score,               # dq_score
            high_f,                 # severity_high_fails
            med_f,                  # severity_med_fails
            low_f,                  # severity_low_fails
            date.today(),           # score_date
            datetime.utcnow(),      # created_timestamp
        )

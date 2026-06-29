"""
================================================================================
Enterprise Data Quality Framework
Component: AuditLogger
================================================================================
Writes execution results to:
    dq_framework.results.execution_history
    dq_framework.results.audit_logs
================================================================================
"""

from __future__ import annotations
import uuid, json
from datetime import datetime
from typing import List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, TimestampType
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.framework_config import (
    TBL_EXECUTION_HISTORY, TBL_AUDIT_LOGS, FRAMEWORK_VERSION
)
from validators.base_validator import ValidationResult


class AuditLogger:
    """
    Writes ValidationResult objects to the execution_history Delta table.
    Also writes operational log messages to audit_logs.
    """

    def __init__(self, spark: SparkSession, run_id: str):
        self._spark  = spark
        self._run_id = run_id

    # ------------------------------------------------------------------ #
    #  Write a single execution result                                     #
    # ------------------------------------------------------------------ #

    def log_result(
        self,
        result: ValidationResult,
        execution_id: str,
        start_time:   datetime,
        end_time:     datetime,
    ):
        duration = (end_time - start_time).total_seconds()

        row = [(
            execution_id,
            self._run_id,
            result.mapping_id,
            result.rule_id,
            result.rule_name,
            result.rule_type,
            result.catalog_name,
            result.schema_name,
            result.table_name,
            result.column_name,
            None,                         # filter_condition stored in history
            result.total_records,
            result.passed_records,
            result.failed_records,
            result.pass_rate,
            result.threshold,
            result.severity,
            result.status,
            result.error_message,
            start_time,
            end_time,
            duration,
            FRAMEWORK_VERSION,
        )]

        schema = StructType([
            StructField("execution_id",      StringType(),    False),
            StructField("run_id",            StringType(),    True),
            StructField("mapping_id",        IntegerType(),   False),
            StructField("rule_id",           IntegerType(),   False),
            StructField("rule_name",         StringType(),    True),
            StructField("rule_type",         StringType(),    True),
            StructField("catalog_name",      StringType(),    True),
            StructField("schema_name",       StringType(),    True),
            StructField("table_name",        StringType(),    True),
            StructField("column_name",       StringType(),    True),
            StructField("filter_condition",  StringType(),    True),
            StructField("total_records",     LongType(),      True),
            StructField("passed_records",    LongType(),      True),
            StructField("failed_records",    LongType(),      True),
            StructField("pass_rate",         DoubleType(),    True),
            StructField("threshold",         DoubleType(),    True),
            StructField("severity",          StringType(),    True),
            StructField("status",            StringType(),    True),
            StructField("error_message",     StringType(),    True),
            StructField("start_time",        TimestampType(), True),
            StructField("end_time",          TimestampType(), True),
            StructField("duration_seconds",  DoubleType(),    True),
            StructField("framework_version", StringType(),    True),
        ])

        df = self._spark.createDataFrame(row, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(TBL_EXECUTION_HISTORY)

    # ------------------------------------------------------------------ #
    #  Batch write results                                                 #
    # ------------------------------------------------------------------ #

    def log_results_batch(self, results_with_timing: list):
        """
        results_with_timing : list of (ValidationResult, execution_id, start, end)
        """
        for result, exec_id, start, end in results_with_timing:
            try:
                self.log_result(result, exec_id, start, end)
            except Exception as e:
                print(f"[AuditLogger] WARNING: Failed to log result for "
                      f"mapping_id={result.mapping_id}: {e}")

    # ------------------------------------------------------------------ #
    #  Operational audit log                                               #
    # ------------------------------------------------------------------ #

    def log(
        self,
        level:      str,
        component:  str,
        message:    str,
        details:    Optional[dict] = None,
        execution_id: Optional[str] = None,
    ):
        row = [(
            str(uuid.uuid4()),
            execution_id,
            self._run_id,
            level,
            component,
            message,
            json.dumps(details) if details else None,
            datetime.utcnow(),
        )]

        schema = StructType([
            StructField("log_id",        StringType(),    False),
            StructField("execution_id",  StringType(),    True),
            StructField("run_id",        StringType(),    True),
            StructField("log_level",     StringType(),    True),
            StructField("component",     StringType(),    True),
            StructField("message",       StringType(),    True),
            StructField("details",       StringType(),    True),
            StructField("log_timestamp", TimestampType(), True),
        ])

        df = self._spark.createDataFrame(row, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(TBL_AUDIT_LOGS)

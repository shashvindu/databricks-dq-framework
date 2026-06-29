"""
================================================================================
Enterprise Data Quality Framework
Component: FailedRecordsWriter
================================================================================
Persists failed rows to  dq_framework.results.failed_records  Delta table.
Captures primary key values and the failing column value for each bad row.
================================================================================
"""

from __future__ import annotations
import uuid, json
from datetime import date
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.framework_config import TBL_FAILED_RECORDS
from validators.base_validator import ValidationResult


class FailedRecordsWriter:
    """
    Writes the failed DataFrame rows from a ValidationResult to the
    dq_framework.results.failed_records Delta table.

    Usage
    -----
    writer = FailedRecordsWriter(spark)
    writer.write(result, execution_id, primary_key="customer_id")
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def write(
        self,
        result:       ValidationResult,
        execution_id: str,
        primary_key:  Optional[str] = None,   # comma-separated PK col names
    ):
        """
        Persist failed records with metadata columns added.
        Silently skips if result.failed_df is None or has 0 rows.
        """
        if result.failed_df is None or result.failed_records == 0:
            return

        failed_df = result.failed_df
        pk_cols   = [c.strip() for c in (primary_key or "").split(",") if c.strip()]
        col_name  = result.column_name

        # Build primary_key_value as JSON string of pk columns
        if pk_cols:
            # Create a map expression: {"col": value, ...}
            map_exprs = []
            for pk in pk_cols:
                if pk in failed_df.columns:
                    map_exprs += [F.lit(pk), F.col(pk).cast(StringType())]
            if map_exprs:
                failed_df = failed_df.withColumn(
                    "_pk_json",
                    F.to_json(F.create_map(*map_exprs))
                )
            else:
                failed_df = failed_df.withColumn("_pk_json", F.lit(None).cast(StringType()))
        else:
            failed_df = failed_df.withColumn("_pk_json", F.lit(None).cast(StringType()))

        # Capture the failing column value
        if col_name and col_name in failed_df.columns:
            failed_df = failed_df.withColumn(
                "_failed_val", F.col(col_name).cast(StringType())
            )
        else:
            failed_df = failed_df.withColumn("_failed_val", F.lit(None).cast(StringType()))

        # Add metadata columns
        output_df = failed_df.select(
            F.lit(str(uuid.uuid4())).alias("failed_record_id"),
            F.lit(execution_id).alias("execution_id"),
            F.lit(result.rule_id).alias("rule_id"),
            F.lit(result.rule_name).alias("rule_name"),
            F.lit(result.catalog_name).alias("catalog_name"),
            F.lit(result.schema_name).alias("schema_name"),
            F.lit(result.table_name).alias("table_name"),
            F.lit(col_name).alias("column_name"),
            F.col("_pk_json").alias("primary_key_value"),
            F.col("_failed_val").alias("failed_column_value"),
            F.lit(result.rule_name).alias("rule_expression"),
            F.lit(result.severity).alias("severity"),
            F.lit(str(date.today())).cast("date").alias("run_date"),
            F.current_timestamp().alias("created_timestamp"),
        )

        # Give each row a unique failed_record_id using monotonically_increasing_id
        output_df = output_df.withColumn(
            "failed_record_id",
            F.concat(F.lit(execution_id + "_"), F.monotonically_increasing_id().cast(StringType()))
        )

        output_df.write.format("delta") \
            .mode("append") \
            .partitionBy("run_date", "catalog_name") \
            .saveAsTable(TBL_FAILED_RECORDS)

        print(f"    [FailedRecordsWriter] Wrote {result.failed_records} "
              f"failed records for rule '{result.rule_name}'")

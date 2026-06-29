"""
================================================================================
Enterprise Data Quality Framework
Validators: Completeness, Uniqueness, Pattern, Range, Domain,
            Referential Integrity, Cross-Column, Aggregate, Custom SQL
================================================================================
All validators follow the Strategy pattern – same interface, different logic.
The RuleEngine picks the right class via a Factory.
================================================================================
"""

from __future__ import annotations
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from validators.base_validator import BaseValidator, ValidationResult


# ============================================================================
#  1.  COMPLETENESS  – NULL / empty / blank checks
# ============================================================================

class CompletenessValidator(BaseValidator):
    """
    Rule types: NULL_CHECK, EMPTY_STRING

    rule_expression examples:
        {col} IS NOT NULL
        {col} IS NOT NULL AND trim({col}) != ""
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col   = mapping.column_name
        expr  = self._resolve_expression(mapping.rule.rule_expression, col, parameters)
        try:
            # Rows that FAIL the expression
            failed_df = df.filter(f"NOT ({expr})")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  2.  UNIQUENESS  – duplicate / composite key checks
# ============================================================================

class UniquenessValidator(BaseValidator):
    """
    Rule types: UNIQUE_CHECK, COMPOSITE_UNIQUE

    column_name can be a comma-separated list for composite keys.
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        cols_raw = mapping.column_name or ""
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
        if not cols:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                cols_raw, mapping.severity, mapping.threshold,
                "No columns specified for uniqueness check"
            )
        try:
            window  = Window.partitionBy(*cols)
            counted = df.withColumn("_dq_cnt", F.count("*").over(window))
            failed_df = counted.filter(F.col("_dq_cnt") > 1).drop("_dq_cnt")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                cols_raw, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  3.  PATTERN  – regex-based validations
# ============================================================================

class PatternValidator(BaseValidator):
    """
    Rule types: EMAIL_REGEX, PHONE_REGEX, CUSTOM_REGEX

    Supports a {pattern} placeholder in the expression for CUSTOM_REGEX.
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col  = mapping.column_name
        expr = self._resolve_expression(mapping.rule.rule_expression, col, parameters)
        try:
            # Only validate non-null rows for pattern rules
            base     = df.filter(F.col(col).isNotNull())
            failed_df = base.filter(f"NOT ({expr})")
            # Add null rows as pass (null handled by completeness rules)
            total    = df.count()
            failed   = failed_df.count()
            passed   = total - failed
            rate     = (passed / total) if total > 0 else 1.0
            status   = "PASS" if rate >= mapping.threshold else "FAIL"
            return ValidationResult(
                mapping_id=mapping.mapping_id, rule_id=mapping.rule_id,
                rule_name=mapping.rule.rule_name, rule_type=mapping.rule.rule_type,
                catalog_name=mapping.table.catalog_name, schema_name=mapping.table.schema_name,
                table_name=mapping.table.table_name, column_name=col,
                total_records=total, passed_records=passed, failed_records=failed,
                pass_rate=rate, threshold=mapping.threshold,
                severity=mapping.severity, status=status, failed_df=failed_df,
            )
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  4.  RANGE  – numeric / date range checks
# ============================================================================

class RangeValidator(BaseValidator):
    """
    Rule types: NUMERIC_BETWEEN, GREATER_THAN, NOT_NEGATIVE, DATE_RANGE

    Parameters: min_val, max_val  (injected via rule_parameters)
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col  = mapping.column_name
        expr = self._resolve_expression(mapping.rule.rule_expression, col, parameters)
        try:
            non_null_df = df.filter(F.col(col).isNotNull())
            failed_df   = non_null_df.filter(f"NOT ({expr})")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  5.  DOMAIN  – value must be in a predefined list
# ============================================================================

class DomainValidator(BaseValidator):
    """
    Rule type: DOMAIN_IN_LIST

    Parameters: allowed_values  (comma-separated, pre-quoted)
    e.g. allowed_values = "'M','F','O'"
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col  = mapping.column_name
        expr = self._resolve_expression(mapping.rule.rule_expression, col, parameters)
        try:
            non_null_df = df.filter(F.col(col).isNotNull())
            failed_df   = non_null_df.filter(f"NOT ({expr})")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  6.  REFERENTIAL INTEGRITY  – FK exists in reference table
# ============================================================================

class ReferentialIntegrityValidator(BaseValidator):
    """
    Rule type: REF_INTEGRITY

    rule_expression token: EXISTS_IN:{ref_catalog}.{ref_schema}.{ref_table}.{ref_column}
    Parameters: ref_catalog, ref_schema, ref_table, ref_column

    Performs an anti-join (left anti) to find orphan FK values.
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col = mapping.column_name
        try:
            ref_catalog = parameters.get("ref_catalog", "")
            ref_schema  = parameters.get("ref_schema",  "")
            ref_table   = parameters.get("ref_table",   "")
            ref_col     = parameters.get("ref_column",  col)
            ref_fq      = f"{ref_catalog}.{ref_schema}.{ref_table}"

            ref_df    = self._spark.table(ref_fq).select(F.col(ref_col).alias("_ref_key")).distinct()
            source_df = df.filter(F.col(col).isNotNull())

            failed_df = source_df.join(
                ref_df,
                on=source_df[col] == ref_df["_ref_key"],
                how="left_anti"
            )
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  7.  CROSS-COLUMN  – multi-column expression
# ============================================================================

class CrossColumnValidator(BaseValidator):
    """
    Rule type: CROSS_COLUMN_EXPR

    Parameters: expression  (a full boolean SQL expression)
    e.g.  start_date <= end_date
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        expr = parameters.get("expression", mapping.rule.rule_expression)
        try:
            failed_df = df.filter(f"NOT ({expr})")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                mapping.column_name, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  8.  AGGREGATE  – table-level aggregate checks
# ============================================================================

class AggregateValidator(BaseValidator):
    """
    Rule types: AGG_ROW_COUNT, AGG_SUM_CHECK, AGG_MIN, AGG_MAX, AGG_AVG

    rule_expression examples:
        COUNT(*) >= {min_count}
        SUM({col}) >= {min_sum}

    Aggregate rules return a single boolean (PASS/FAIL for the whole table).
    total_records and failed_records are set to 1 and 0|1 respectively.
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        col  = mapping.column_name
        expr = self._resolve_expression(mapping.rule.rule_expression, col or "*", parameters)
        try:
            # Evaluate: does the aggregate satisfy the condition?
            agg_sql = f"SELECT CASE WHEN {expr} THEN 1 ELSE 0 END AS _dq_pass FROM ({df._jdf.toString()})"
            result_row = df.selectExpr(f"CASE WHEN {expr} THEN 1 ELSE 0 END AS _dq_pass").collect()

            # Simpler approach: build the aggregate expression directly
            agg_check = df.agg(F.expr(f"CASE WHEN {expr} THEN 1 ELSE 0 END").alias("_dq_pass"))
            pass_val  = agg_check.collect()[0]["_dq_pass"]
            total     = df.count()
            failed    = 0 if pass_val == 1 else total
            passed    = total - failed
            rate      = 1.0 if pass_val == 1 else 0.0
            status    = "PASS" if rate >= mapping.threshold else "FAIL"

            return ValidationResult(
                mapping_id=mapping.mapping_id, rule_id=mapping.rule_id,
                rule_name=mapping.rule.rule_name, rule_type=mapping.rule.rule_type,
                catalog_name=mapping.table.catalog_name, schema_name=mapping.table.schema_name,
                table_name=mapping.table.table_name, column_name=col,
                total_records=total, passed_records=passed, failed_records=failed,
                pass_rate=rate, threshold=mapping.threshold,
                severity=mapping.severity, status=status, failed_df=None,
            )
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                col, mapping.severity, mapping.threshold, str(e)
            )


# ============================================================================
#  9.  CUSTOM SQL  – fully flexible SQL boolean expression
# ============================================================================

class CustomSQLValidator(BaseValidator):
    """
    Rule type: CUSTOM_SQL

    Parameters: sql_expression  (boolean SQL expression per row)
    e.g. amount > 0 OR currency IS NOT NULL
    """

    def validate(self, df: DataFrame, mapping, parameters: Dict) -> ValidationResult:
        sql_expr = parameters.get("sql_expression", mapping.rule.rule_expression)
        try:
            failed_df = df.filter(f"NOT ({sql_expr})")
            return self._compute_result(mapping, df, failed_df)
        except Exception as e:
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                mapping.column_name, mapping.severity, mapping.threshold, str(e)
            )

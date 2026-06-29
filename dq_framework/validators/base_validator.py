"""
================================================================================
Enterprise Data Quality Framework
Component: BaseValidator (Abstract)
================================================================================
Every rule-type validator inherits from BaseValidator and implements
the  validate()  method. The framework never instantiates BaseValidator
directly – it uses the RuleEngine factory to pick the right subclass.
================================================================================
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict
from pyspark.sql import DataFrame, SparkSession


@dataclass
class ValidationResult:
    """
    Immutable result object returned by every validator.
    """
    mapping_id:       int
    rule_id:          int
    rule_name:        str
    rule_type:        str
    catalog_name:     str
    schema_name:      str
    table_name:       str
    column_name:      Optional[str]
    total_records:    int
    passed_records:   int
    failed_records:   int
    pass_rate:        float
    threshold:        float
    severity:         str
    status:           str              # PASS | FAIL | ERROR | SKIPPED
    error_message:    Optional[str] = None
    failed_df:        Optional[DataFrame] = field(default=None, repr=False)
    # status is set after comparing pass_rate vs threshold

    @classmethod
    def error(cls, mapping_id, rule_id, rule_name, rule_type,
              catalog, schema, table, column, severity, threshold, msg) -> "ValidationResult":
        return cls(
            mapping_id=mapping_id, rule_id=rule_id,
            rule_name=rule_name, rule_type=rule_type,
            catalog_name=catalog, schema_name=schema,
            table_name=table, column_name=column,
            total_records=0, passed_records=0, failed_records=0,
            pass_rate=0.0, threshold=threshold,
            severity=severity, status="ERROR", error_message=msg,
        )


class BaseValidator(ABC):
    """
    Abstract base class for all DQ rule validators.

    Subclasses must implement:
        validate(df, mapping, parameters) -> ValidationResult

    Parameters
    ----------
    spark : Active SparkSession
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @abstractmethod
    def validate(
        self,
        df: DataFrame,
        mapping,           # MappingMeta
        parameters: Dict[str, str]
    ) -> ValidationResult:
        ...

    # ------------------------------------------------------------------ #
    #  Shared helpers available to all subclasses                          #
    # ------------------------------------------------------------------ #

    def _resolve_expression(self, template: str, column: Optional[str],
                            parameters: Dict[str, str]) -> str:
        """
        Replace {col} and any {key} placeholders in a rule_expression template.

        Example
        -------
        template   = '{col} BETWEEN {min_val} AND {max_val}'
        column     = 'age'
        parameters = {'min_val': '0', 'max_val': '120'}
        → 'age BETWEEN 0 AND 120'
        """
        expr = template
        if column:
            expr = expr.replace("{col}", column)
        for k, v in parameters.items():
            expr = expr.replace(f"{{{k}}}", v)
        return expr

    def _compute_result(
        self,
        mapping,
        df: DataFrame,
        failed_df: DataFrame,
    ) -> ValidationResult:
        """
        Count total vs failed rows and build a ValidationResult.
        """
        total   = df.count()
        failed  = failed_df.count()
        passed  = total - failed
        rate    = (passed / total) if total > 0 else 1.0
        status  = "PASS" if rate >= mapping.threshold else "FAIL"

        return ValidationResult(
            mapping_id=mapping.mapping_id,
            rule_id=mapping.rule_id,
            rule_name=mapping.rule.rule_name,
            rule_type=mapping.rule.rule_type,
            catalog_name=mapping.table.catalog_name,
            schema_name=mapping.table.schema_name,
            table_name=mapping.table.table_name,
            column_name=mapping.column_name,
            total_records=total,
            passed_records=passed,
            failed_records=failed,
            pass_rate=rate,
            threshold=mapping.threshold,
            severity=mapping.severity,
            status=status,
            failed_df=failed_df,
        )

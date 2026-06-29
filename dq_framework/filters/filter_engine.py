"""
================================================================================
Enterprise Data Quality Framework
Component: FilterEngine
================================================================================
Applies a dynamic filter condition (optional WHERE clause) to a DataFrame.
The condition is a raw SQL expression string that comes from the
rule_mapping.filter_condition column.
================================================================================
"""

from __future__ import annotations
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class FilterEngine:
    """
    Applies optional filter conditions to source DataFrames before rule validation.

    Usage
    -----
    engine = FilterEngine()
    df_filtered = engine.apply(df, "country = 'US' AND status = 'ACTIVE'")
    """

    def apply(self, df: DataFrame, filter_condition: Optional[str]) -> DataFrame:
        """
        Apply a SQL WHERE expression to df.
        Returns df unchanged if filter_condition is None or empty.
        """
        if not filter_condition or not filter_condition.strip():
            return df

        condition = filter_condition.strip()
        print(f"[FilterEngine] Applying filter: {condition}")
        try:
            return df.filter(condition)
        except Exception as e:
            raise ValueError(
                f"[FilterEngine] Failed to apply filter '{condition}': {e}"
            ) from e

    def apply_partition_filter(
        self, df: DataFrame, partition_col: str, partition_value: str
    ) -> DataFrame:
        """
        Convenience method for a single partition column filter.
        Useful for pushing down date partition filters before any joins.
        """
        expr = f"{partition_col} = '{partition_value}'"
        return self.apply(df, expr)

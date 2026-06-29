"""
================================================================================
Enterprise Data Quality Framework
Component: JoinEngine
================================================================================
Applies a configurable chain of Delta table joins to a base DataFrame.
All join metadata is read from dq_framework.config.join_config – nothing is
hardcoded here.
================================================================================
"""

from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metadata.metadata_loader import JoinMeta
from readers.delta_reader import DeltaReader


class JoinEngine:
    """
    Applies a multi-hop join chain defined in join_config to a base DataFrame.

    Usage
    -----
    engine = JoinEngine(reader)
    df_joined = engine.apply(base_df, join_chain=[join_meta_1, join_meta_2, …])
    """

    def __init__(self, reader: DeltaReader):
        self._reader = reader

    def apply(self, base_df: DataFrame, join_chain: List[JoinMeta]) -> DataFrame:
        """
        Iteratively apply each JoinMeta in the ordered chain to base_df.

        Parameters
        ----------
        base_df     : Starting DataFrame (the primary table being validated)
        join_chain  : Ordered list of JoinMeta objects (ascending join_order)

        Returns
        -------
        DataFrame with all joins applied.
        """
        if not join_chain:
            return base_df

        result = base_df
        for jm in sorted(join_chain, key=lambda j: j.join_order):
            print(f"[JoinEngine] Applying {jm.join_type} JOIN: {jm.left_fq} ↔ {jm.right_fq}")
            right_df = self._reader.read(jm.right_fq)

            # Avoid ambiguous column names by aliasing DataFrames
            left_alias  = f"l{jm.join_order}"
            right_alias = f"r{jm.join_order}"

            result = (
                result.alias(left_alias)
                .join(
                    right_df.alias(right_alias),
                    on=jm.join_condition,
                    how=jm.join_type.lower()
                )
            )
        return result

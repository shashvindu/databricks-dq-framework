"""
================================================================================
Enterprise Data Quality Framework
Component: DeltaReader
================================================================================
Reads source Delta tables from Unity Catalog and caches them in Spark memory
so that multiple rules targeting the same table re-use the cached DataFrame.
================================================================================
"""

from __future__ import annotations
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.framework_config import CACHE_SOURCE_TABLES


class DeltaReader:
    """
    Singleton-style DataFrame cache for source Delta tables.

    Usage
    -----
    reader = DeltaReader(spark)
    df = reader.read("dq_framework.staging.customer")
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._cache: Dict[str, DataFrame] = {}

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def read(self, fq_table_name: str) -> DataFrame:
        """
        Return a DataFrame for the given fully-qualified table name.
        If the table was already read in this session it is returned from cache.
        """
        key = fq_table_name.lower()
        if key not in self._cache:
            print(f"[DeltaReader] Reading table: {fq_table_name}")
            df = self._spark.table(fq_table_name)
            if CACHE_SOURCE_TABLES:
                df = df.cache()
                df.count()   # materialise the cache
                print(f"[DeltaReader] Cached: {fq_table_name}")
            self._cache[key] = df
        else:
            print(f"[DeltaReader] Cache hit: {fq_table_name}")
        return self._cache[key]

    def read_custom_sql(self, sql: str) -> DataFrame:
        """Execute an arbitrary SQL query and return the DataFrame."""
        return self._spark.sql(sql)

    def evict(self, fq_table_name: str):
        """Remove a table from cache (useful for large tables after all rules run)."""
        key = fq_table_name.lower()
        if key in self._cache:
            try:
                self._cache[key].unpersist()
            except Exception:
                pass
            del self._cache[key]
            print(f"[DeltaReader] Evicted from cache: {fq_table_name}")

    def evict_all(self):
        """Evict all cached DataFrames."""
        for key in list(self._cache.keys()):
            try:
                self._cache[key].unpersist()
            except Exception:
                pass
        self._cache.clear()
        print("[DeltaReader] All caches evicted.")

    @property
    def cached_tables(self):
        return list(self._cache.keys())

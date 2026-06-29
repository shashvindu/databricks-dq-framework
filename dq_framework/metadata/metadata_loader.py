"""
================================================================================
Enterprise Data Quality Framework
Component: MetadataLoader
================================================================================
Reads all configuration from Delta metadata tables and exposes them as
Python dataclasses / dictionaries so the rest of the framework has a clean,
typed view of the metadata without raw DataFrame manipulation everywhere.
================================================================================
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.framework_config import (
    TBL_CONFIG_TABLES, TBL_CONFIG_RULES, TBL_RULE_MAPPING,
    TBL_RULE_PARAMETERS, TBL_JOIN_CONFIG
)


# ---------------------------------------------------------------------------
# Data classes – typed views of metadata rows
# ---------------------------------------------------------------------------

@dataclass
class TableMeta:
    table_id:     int
    catalog_name: str
    schema_name:  str
    table_name:   str
    primary_key:  Optional[str]   # comma-separated PK columns
    fq_name: str = field(init=False)

    def __post_init__(self):
        self.fq_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"


@dataclass
class RuleMeta:
    rule_id:          int
    rule_name:        str
    rule_type:        str          # COMPLETENESS | UNIQUENESS | …
    rule_expression:  str


@dataclass
class MappingMeta:
    mapping_id:       int
    rule_id:          int
    table_id:         int
    column_name:      Optional[str]
    filter_condition: Optional[str]
    join_id:          Optional[int]
    severity:         str
    threshold:        float
    execution_order:  int
    parameters:       Dict[str, str] = field(default_factory=dict)
    # resolved at load time
    rule:             Optional[RuleMeta]   = None
    table:            Optional[TableMeta]  = None


@dataclass
class JoinMeta:
    join_id:        int
    join_name:      str
    left_fq:        str    # catalog.schema.table
    right_fq:       str
    join_type:      str    # INNER | LEFT | RIGHT | FULL
    join_condition: str
    join_order:     int


# ---------------------------------------------------------------------------
# MetadataLoader
# ---------------------------------------------------------------------------

class MetadataLoader:
    """
    Single entry-point for loading all DQ metadata from Delta tables.

    Usage
    -----
    loader = MetadataLoader(spark)
    loader.load()

    # Get all active mappings for a specific table
    mappings = loader.get_mappings_for_table(table_id=1)
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._tables:     Dict[int, TableMeta]   = {}
        self._rules:      Dict[int, RuleMeta]    = {}
        self._mappings:   List[MappingMeta]      = []
        self._parameters: Dict[int, Dict]        = {}   # mapping_id → {key: val}
        self._joins:      Dict[int, List[JoinMeta]] = {}  # join_id → ordered list
        self._loaded      = False

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def load(self) -> "MetadataLoader":
        """Load all metadata from Delta tables. Call once per job run."""
        print("[MetadataLoader] Loading metadata …")
        self._load_tables()
        self._load_rules()
        self._load_parameters()
        self._load_joins()
        self._load_mappings()
        self._loaded = True
        print(f"[MetadataLoader] Loaded: "
              f"{len(self._tables)} tables | "
              f"{len(self._rules)} rules | "
              f"{len(self._mappings)} active mappings")
        return self

    def get_all_mappings(self) -> List[MappingMeta]:
        self._ensure_loaded()
        return self._mappings

    def get_mappings_for_table(self, table_id: int) -> List[MappingMeta]:
        self._ensure_loaded()
        return sorted(
            [m for m in self._mappings if m.table_id == table_id],
            key=lambda m: m.execution_order
        )

    def get_table(self, table_id: int) -> Optional[TableMeta]:
        return self._tables.get(table_id)

    def get_all_table_ids(self) -> List[int]:
        """Returns unique table IDs that have at least one active mapping."""
        self._ensure_loaded()
        return list({m.table_id for m in self._mappings})

    def get_joins(self, join_id: int) -> List[JoinMeta]:
        """Returns the ordered join chain for a given join_id."""
        self._ensure_loaded()
        return self._joins.get(join_id, [])

    # ------------------------------------------------------------------ #
    #  Private loaders                                                     #
    # ------------------------------------------------------------------ #

    def _load_tables(self):
        rows = self._spark.table(TBL_CONFIG_TABLES) \
            .filter(F.col("active") == True) \
            .select("table_id", "catalog_name", "schema_name", "table_name", "primary_key") \
            .collect()
        for r in rows:
            self._tables[r.table_id] = TableMeta(
                table_id=r.table_id,
                catalog_name=r.catalog_name,
                schema_name=r.schema_name,
                table_name=r.table_name,
                primary_key=r.primary_key,
            )

    def _load_rules(self):
        rows = self._spark.table(TBL_CONFIG_RULES) \
            .filter(F.col("active") == True) \
            .select("rule_id", "rule_name", "rule_type", "rule_expression") \
            .collect()
        for r in rows:
            self._rules[r.rule_id] = RuleMeta(
                rule_id=r.rule_id,
                rule_name=r.rule_name,
                rule_type=r.rule_type,
                rule_expression=r.rule_expression,
            )

    def _load_parameters(self):
        rows = self._spark.table(TBL_RULE_PARAMETERS) \
            .select("mapping_id", "param_key", "param_value") \
            .collect()
        for r in rows:
            if r.mapping_id not in self._parameters:
                self._parameters[r.mapping_id] = {}
            self._parameters[r.mapping_id][r.param_key] = r.param_value

    def _load_joins(self):
        rows = self._spark.table(TBL_JOIN_CONFIG) \
            .filter(F.col("active") == True) \
            .select("join_id", "join_name",
                    "left_catalog", "left_schema", "left_table",
                    "right_catalog", "right_schema", "right_table",
                    "join_type", "join_condition", "join_order") \
            .orderBy("join_id", "join_order") \
            .collect()
        for r in rows:
            jm = JoinMeta(
                join_id=r.join_id,
                join_name=r.join_name,
                left_fq=f"{r.left_catalog}.{r.left_schema}.{r.left_table}",
                right_fq=f"{r.right_catalog}.{r.right_schema}.{r.right_table}",
                join_type=r.join_type,
                join_condition=r.join_condition,
                join_order=r.join_order,
            )
            if r.join_id not in self._joins:
                self._joins[r.join_id] = []
            self._joins[r.join_id].append(jm)

    def _load_mappings(self):
        rows = self._spark.table(TBL_RULE_MAPPING) \
            .filter(F.col("active") == True) \
            .select("mapping_id", "rule_id", "table_id", "column_name",
                    "filter_condition", "join_id", "severity", "threshold",
                    "execution_order") \
            .orderBy("table_id", "execution_order") \
            .collect()

        from config.framework_config import DEFAULT_SEVERITY, DEFAULT_THRESHOLD
        for r in rows:
            rule  = self._rules.get(r.rule_id)
            table = self._tables.get(r.table_id)
            if rule is None or table is None:
                print(f"[MetadataLoader] WARNING: mapping_id={r.mapping_id} "
                      f"references missing rule_id={r.rule_id} or table_id={r.table_id} – skipped")
                continue

            mapping = MappingMeta(
                mapping_id=r.mapping_id,
                rule_id=r.rule_id,
                table_id=r.table_id,
                column_name=r.column_name,
                filter_condition=r.filter_condition,
                join_id=r.join_id,
                severity=r.severity or DEFAULT_SEVERITY,
                threshold=r.threshold if r.threshold is not None else DEFAULT_THRESHOLD,
                execution_order=r.execution_order or 100,
                parameters=self._parameters.get(r.mapping_id, {}),
                rule=rule,
                table=table,
            )
            self._mappings.append(mapping)

    def _ensure_loaded(self):
        if not self._loaded:
            raise RuntimeError("Call MetadataLoader.load() before using this method.")

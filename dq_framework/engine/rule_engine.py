"""
================================================================================
Enterprise Data Quality Framework
Component: RuleEngine  (Factory + Strategy Pattern)
================================================================================
The RuleEngine is the core orchestrator for a SINGLE mapping execution.

Factory Pattern  – get_validator(rule_type) picks the right validator class.
Strategy Pattern – every validator implements the same validate() interface.

The RuleEngine:
  1. Resolves the validator from rule type
  2. Applies join (if configured)
  3. Applies filter (if configured)
  4. Calls validator.validate()
  5. Returns ValidationResult

The DQOrchestrator loops over all mappings and calls RuleEngine.execute().
================================================================================
"""

from __future__ import annotations
from typing import Dict, Type
from pyspark.sql import SparkSession, DataFrame

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metadata.metadata_loader import MetadataLoader, MappingMeta
from readers.delta_reader      import DeltaReader
from joins.join_engine         import JoinEngine
from filters.filter_engine     import FilterEngine
from validators.base_validator import BaseValidator, ValidationResult
from validators.all_validators import (
    CompletenessValidator,
    UniquenessValidator,
    PatternValidator,
    RangeValidator,
    DomainValidator,
    ReferentialIntegrityValidator,
    CrossColumnValidator,
    AggregateValidator,
    CustomSQLValidator,
)


# ---------------------------------------------------------------------------
# Registry – maps rule_type string → validator class
# To add a new rule type: just add an entry here. Zero other code changes.
# ---------------------------------------------------------------------------
VALIDATOR_REGISTRY: Dict[str, Type[BaseValidator]] = {
    "COMPLETENESS":   CompletenessValidator,
    "UNIQUENESS":     UniquenessValidator,
    "PATTERN":        PatternValidator,
    "RANGE":          RangeValidator,
    "DOMAIN":         DomainValidator,
    "REF_INTEGRITY":  ReferentialIntegrityValidator,
    "CROSS_COLUMN":   CrossColumnValidator,
    "AGGREGATE":      AggregateValidator,
    "CUSTOM_SQL":     CustomSQLValidator,
    # Future:
    # "CROSS_TABLE":  CrossTableValidator,
    # "STATISTICAL":  StatisticalValidator,
}


class RuleEngine:
    """
    Executes a single DQ rule mapping end-to-end.

    Parameters
    ----------
    spark         : Active SparkSession
    metadata      : Loaded MetadataLoader instance
    reader        : DeltaReader with table cache
    join_engine   : JoinEngine
    filter_engine : FilterEngine
    """

    def __init__(
        self,
        spark:         SparkSession,
        metadata:      MetadataLoader,
        reader:        DeltaReader,
        join_engine:   JoinEngine,
        filter_engine: FilterEngine,
    ):
        self._spark         = spark
        self._metadata      = metadata
        self._reader        = reader
        self._join_engine   = join_engine
        self._filter_engine = filter_engine

    # ------------------------------------------------------------------ #
    #  Factory: get the right validator                                    #
    # ------------------------------------------------------------------ #

    def get_validator(self, rule_type: str) -> BaseValidator:
        klass = VALIDATOR_REGISTRY.get(rule_type.upper())
        if klass is None:
            raise ValueError(
                f"[RuleEngine] Unknown rule_type '{rule_type}'. "
                f"Registered types: {list(VALIDATOR_REGISTRY.keys())}"
            )
        return klass(self._spark)

    # ------------------------------------------------------------------ #
    #  Main entry point                                                    #
    # ------------------------------------------------------------------ #

    def execute(self, mapping: MappingMeta, base_df: DataFrame) -> ValidationResult:
        """
        Execute a single rule mapping against base_df.

        Steps
        -----
        1. Apply joins (if mapping.join_id is set)
        2. Apply filter (if mapping.filter_condition is set)
        3. Pick validator via factory
        4. Call validator.validate()
        5. Return ValidationResult
        """
        rule_name = mapping.rule.rule_name
        tbl_name  = mapping.table.fq_name
        col_name  = mapping.column_name or "TABLE_LEVEL"

        print(f"  → [RuleEngine] Executing rule '{rule_name}' "
              f"on {tbl_name}.{col_name}")

        try:
            # Step 1: Apply joins
            working_df = base_df
            if mapping.join_id is not None:
                join_chain = self._metadata.get_joins(mapping.join_id)
                if join_chain:
                    working_df = self._join_engine.apply(working_df, join_chain)

            # Step 2: Apply filter
            working_df = self._filter_engine.apply(working_df, mapping.filter_condition)

            # Step 3: Pick validator (Factory pattern)
            validator = self.get_validator(mapping.rule.rule_type)

            # Step 4: Execute validation (Strategy pattern)
            result = validator.validate(working_df, mapping, mapping.parameters)
            return result

        except Exception as e:
            print(f"  ✗ [RuleEngine] ERROR in rule '{rule_name}': {e}")
            return ValidationResult.error(
                mapping.mapping_id, mapping.rule_id, mapping.rule.rule_name,
                mapping.rule.rule_type, mapping.table.catalog_name,
                mapping.table.schema_name, mapping.table.table_name,
                mapping.column_name, mapping.severity, mapping.threshold,
                str(e)
            )

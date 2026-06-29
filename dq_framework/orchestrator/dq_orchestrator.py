"""
================================================================================
Enterprise Data Quality Framework
Component: DQOrchestrator
================================================================================
The top-level coordinator that:
  1. Reads all active mappings from MetadataLoader
  2. Groups mappings by source table (to read each Delta table only once)
  3. For each table: loads the DataFrame, then executes each rule via RuleEngine
  4. Collects ValidationResults
  5. Writes failed records via FailedRecordsWriter
  6. Writes audit logs via AuditLogger
  7. Computes DQ scores via DQScoreEngine
================================================================================
"""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import List, Dict, Optional

from pyspark.sql import SparkSession

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metadata.metadata_loader    import MetadataLoader
from readers.delta_reader        import DeltaReader
from joins.join_engine           import JoinEngine
from filters.filter_engine       import FilterEngine
from engine.rule_engine          import RuleEngine
from audit.audit_logger          import AuditLogger
from failed_records.failed_records_writer import FailedRecordsWriter
from scoring.dq_score_engine     import DQScoreEngine
from validators.base_validator   import ValidationResult


class DQOrchestrator:
    """
    Main entry point for a full DQ framework run.

    Usage
    -----
    orch = DQOrchestrator(spark, run_id="databricks-job-run-123")
    orch.run()
    """

    def __init__(self, spark: SparkSession, run_id: Optional[str] = None):
        self._spark   = spark
        self._run_id  = run_id or str(uuid.uuid4())[:8]

        # Initialise all components
        self._metadata     = MetadataLoader(spark)
        self._reader       = DeltaReader(spark)
        self._join_engine  = JoinEngine(self._reader)
        self._filter_engine = FilterEngine()
        self._rule_engine  = RuleEngine(
            spark, self._metadata, self._reader,
            self._join_engine, self._filter_engine
        )
        self._audit        = AuditLogger(spark, self._run_id)
        self._fr_writer    = FailedRecordsWriter(spark)
        self._score_engine = DQScoreEngine(spark, self._run_id)

    # ------------------------------------------------------------------ #
    #  Public run method                                                   #
    # ------------------------------------------------------------------ #

    def run(
        self,
        table_ids:       Optional[List[int]] = None,   # None = run all tables
        rule_types:      Optional[List[str]] = None,   # None = run all rule types
        severities:      Optional[List[str]] = None,   # None = run all severities
    ) -> List[ValidationResult]:
        """
        Execute the full DQ framework run.

        Parameters
        ----------
        table_ids   : Limit execution to specific table IDs (optional)
        rule_types  : Limit execution to specific rule types (optional)
        severities  : Limit execution to specific severities (optional)

        Returns
        -------
        List of ValidationResult objects
        """
        run_start = datetime.utcnow()
        print("=" * 70)
        print(f"DQ Framework Run Started  |  run_id={self._run_id}")
        print(f"Timestamp: {run_start.isoformat()}")
        print("=" * 70)

        # Load metadata
        self._metadata.load()

        # Resolve target table IDs
        all_table_ids = (
            table_ids
            if table_ids is not None
            else self._metadata.get_all_table_ids()
        )

        all_results: List[ValidationResult] = []
        results_with_timing = []

        for table_id in all_table_ids:
            table_meta = self._metadata.get_table(table_id)
            if table_meta is None:
                print(f"[DQOrchestrator] WARNING: table_id={table_id} not found – skipped")
                continue

            print(f"\n{'─'*70}")
            print(f"[Table] {table_meta.fq_name}")
            print(f"{'─'*70}")

            # Load source DataFrame ONCE for this table
            try:
                base_df = self._reader.read(table_meta.fq_name)
            except Exception as e:
                print(f"[DQOrchestrator] ERROR: Cannot read {table_meta.fq_name}: {e}")
                self._audit.log("ERROR", "DQOrchestrator",
                                f"Cannot read table {table_meta.fq_name}", {"error": str(e)})
                continue

            # Get all mappings for this table (sorted by execution_order)
            mappings = self._metadata.get_mappings_for_table(table_id)

            # Apply optional filters
            if rule_types:
                mappings = [m for m in mappings if m.rule.rule_type in rule_types]
            if severities:
                mappings = [m for m in mappings if m.severity in severities]

            print(f"  Rules to execute: {len(mappings)}")

            table_results = []
            for mapping in mappings:
                exec_id    = str(uuid.uuid4())
                start_time = datetime.utcnow()

                result = self._rule_engine.execute(mapping, base_df)

                end_time   = datetime.utcnow()
                duration   = (end_time - start_time).total_seconds()

                status_icon = "✓" if result.status == "PASS" else ("✗" if result.status == "FAIL" else "⚠")
                print(f"    {status_icon} [{result.status:7s}] "
                      f"{result.rule_name:25s} "
                      f"col={result.column_name or 'N/A':20s} "
                      f"pass_rate={result.pass_rate:.1%} "
                      f"threshold={result.threshold:.1%} "
                      f"({duration:.2f}s)")

                # Write failed records
                if result.status == "FAIL" and result.failed_df is not None:
                    self._fr_writer.write(result, exec_id, table_meta.primary_key)

                # Write audit log
                try:
                    self._audit.log_result(result, exec_id, start_time, end_time)
                except Exception as e:
                    print(f"    [AuditLogger] WARNING: Could not write audit log: {e}")

                table_results.append(result)
                all_results.append(result)
                results_with_timing.append((result, exec_id, start_time, end_time))

            # Summary for this table
            passed = sum(1 for r in table_results if r.status == "PASS")
            failed = sum(1 for r in table_results if r.status == "FAIL")
            errors = sum(1 for r in table_results if r.status == "ERROR")
            print(f"\n  Table Summary: {len(table_results)} rules — "
                  f"✓{passed} passed | ✗{failed} failed | ⚠{errors} errors")

        # Compute & store DQ scores
        exec_id_for_score = results_with_timing[0][1] if results_with_timing else str(uuid.uuid4())
        try:
            self._score_engine.compute_and_save(all_results, exec_id_for_score)
        except Exception as e:
            print(f"[DQOrchestrator] WARNING: DQ score writing failed: {e}")

        # Evict Spark cache
        self._reader.evict_all()

        # Final summary
        run_end  = datetime.utcnow()
        run_time = (run_end - run_start).total_seconds()
        total_p  = sum(1 for r in all_results if r.status == "PASS")
        total_f  = sum(1 for r in all_results if r.status == "FAIL")
        total_e  = sum(1 for r in all_results if r.status == "ERROR")

        print(f"\n{'='*70}")
        print(f"DQ Framework Run Complete  |  run_id={self._run_id}")
        print(f"Total rules: {len(all_results)}  |  "
              f"✓ Passed: {total_p}  |  ✗ Failed: {total_f}  |  ⚠ Errors: {total_e}")
        print(f"Total runtime: {run_time:.1f}s")
        print(f"{'='*70}\n")

        return all_results

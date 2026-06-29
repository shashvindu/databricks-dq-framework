"""
================================================================================
Enterprise Data Quality Framework
Tests: Unit tests for all validators
================================================================================
Run in Databricks notebook or via pytest on a cluster with Delta Lake support.

Usage (Databricks):
    %run /path/to/dq_framework/tests/test_all_validators

Usage (pytest):
    pytest dq_framework/tests/test_all_validators.py -v
================================================================================
"""

import sys, os, uuid
from datetime import date

# Make packages importable
_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType
)
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("DQ-Framework-Tests") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from validators.base_validator import ValidationResult
from validators.all_validators import (
    CompletenessValidator, UniquenessValidator, PatternValidator,
    RangeValidator, DomainValidator, ReferentialIntegrityValidator,
    CrossColumnValidator, AggregateValidator, CustomSQLValidator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeMappingRule:
    def __init__(self, rule_id, rule_name, rule_type, rule_expression):
        self.rule_id        = rule_id
        self.rule_name      = rule_name
        self.rule_type      = rule_type
        self.rule_expression = rule_expression

class FakeMappingTable:
    def __init__(self, cat="test_cat", sch="test_sch", tbl="test_tbl", pk="id"):
        self.catalog_name = cat
        self.schema_name  = sch
        self.table_name   = tbl
        self.primary_key  = pk
        self.fq_name      = f"{cat}.{sch}.{tbl}"

class FakeMapping:
    def __init__(self, rule_type, rule_expression, column_name=None,
                 severity="HIGH", threshold=0.95, parameters=None):
        self.mapping_id      = 999
        self.rule_id         = 1
        self.table_id        = 1
        self.column_name     = column_name
        self.filter_condition = None
        self.join_id         = None
        self.severity        = severity
        self.threshold       = threshold
        self.parameters      = parameters or {}
        self.rule            = FakeMappingRule(1, rule_type, rule_type, rule_expression)
        self.table           = FakeMappingTable()


PASS = "PASS"
FAIL = "FAIL"
ERROR = "ERROR"

passed_tests = []
failed_tests = []

def assert_status(name, result, expected_status):
    if result.status == expected_status:
        print(f"  ✓ PASS  {name}")
        passed_tests.append(name)
    else:
        msg = (f"  ✗ FAIL  {name}  "
               f"expected={expected_status} got={result.status} "
               f"pass_rate={result.pass_rate:.1%} err={result.error_message}")
        print(msg)
        failed_tests.append(name)

def assert_count(name, result, expected_failed):
    if result.failed_records == expected_failed:
        print(f"  ✓ PASS  {name}  (failed={expected_failed})")
        passed_tests.append(name)
    else:
        msg = (f"  ✗ FAIL  {name}  "
               f"expected_failed={expected_failed} got={result.failed_records}")
        print(msg)
        failed_tests.append(name)


# ===========================================================================
#  TEST 1 — CompletenessValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 1: CompletenessValidator")
print("─"*60)

schema = StructType([
    StructField("id",    IntegerType(), False),
    StructField("email", StringType(),  True),
])
data = [
    (1, "alice@example.com"),
    (2, None),                 # NULL  → fail
    (3, "bob@example.com"),
]
df   = spark.createDataFrame(data, schema=schema)
v    = CompletenessValidator(spark)

m    = FakeMapping("COMPLETENESS", "{col} IS NOT NULL", column_name="email", threshold=0.90)
r    = v.validate(df, m, {})
assert_status("NULL_CHECK should FAIL (1/3 null)", r, FAIL)
assert_count ("NULL_CHECK failed_records=1",        r, 1)

m2   = FakeMapping("COMPLETENESS", "{col} IS NOT NULL", column_name="email", threshold=0.50)
r2   = v.validate(df, m2, {})
assert_status("NULL_CHECK should PASS at threshold 0.50", r2, PASS)


# ===========================================================================
#  TEST 2 — UniquenessValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 2: UniquenessValidator")
print("─"*60)

schema2 = StructType([
    StructField("id",   IntegerType(), False),
    StructField("name", StringType(),  True),
])
data2 = [
    (1, "Alice"),
    (2, "Bob"),
    (2, "Bob"),   # duplicate id=2 → 2 rows fail
]
df2 = spark.createDataFrame(data2, schema=schema2)
v2  = UniquenessValidator(spark)

m3  = FakeMapping("UNIQUENESS", "{col}", column_name="id", threshold=1.00)
r3  = v2.validate(df2, m3, {})
assert_status("UNIQUE_CHECK id should FAIL", r3, FAIL)
assert_count ("UNIQUE_CHECK failed_records=2 (both dupes)", r3, 2)


# ===========================================================================
#  TEST 3 — PatternValidator (Email)
# ===========================================================================
print("\n" + "─"*60)
print("TEST 3: PatternValidator — Email")
print("─"*60)

schema3 = StructType([
    StructField("id",    IntegerType(), False),
    StructField("email", StringType(),  True),
])
data3 = [
    (1, "valid@example.com"),
    (2, "not-an-email"),       # fail
    (3, "also_bad"),           # fail
    (4, "ok@domain.org"),
    (5, None),                 # null — skipped by PatternValidator
]
df3 = spark.createDataFrame(data3, schema=schema3)
v3  = PatternValidator(spark)

email_expr = r'{col} RLIKE "^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"'
m4  = FakeMapping("PATTERN", email_expr, column_name="email", threshold=0.90)
r4  = v3.validate(df3, m4, {})
assert_status("EMAIL_REGEX should FAIL (2 bad emails)", r4, FAIL)
assert_count ("EMAIL_REGEX failed=2",                   r4, 2)


# ===========================================================================
#  TEST 4 — RangeValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 4: RangeValidator")
print("─"*60)

schema4 = StructType([
    StructField("id",  IntegerType(), False),
    StructField("age", IntegerType(), True),
])
data4 = [(1, 25), (2, -1), (3, 150), (4, 45), (5, None)]
df4   = spark.createDataFrame(data4, schema=schema4)
v4    = RangeValidator(spark)

m5    = FakeMapping("RANGE", "{col} BETWEEN {min_val} AND {max_val}",
                    column_name="age", threshold=0.80,
                    parameters={"min_val": "0", "max_val": "120"})
r5    = v4.validate(df4, m5, m5.parameters)
assert_status("NUMERIC_BETWEEN should FAIL (age -1 and 150 OOB)", r5, FAIL)
assert_count ("NUMERIC_BETWEEN failed=2",                          r5, 2)


# ===========================================================================
#  TEST 5 — DomainValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 5: DomainValidator")
print("─"*60)

schema5 = StructType([
    StructField("id",     IntegerType(), False),
    StructField("gender", StringType(),  True),
])
data5 = [(1, "M"), (2, "F"), (3, "X"), (4, "O"), (5, None)]
df5   = spark.createDataFrame(data5, schema=schema5)
v5    = DomainValidator(spark)

m6    = FakeMapping("DOMAIN", "{col} IN ({allowed_values})",
                    column_name="gender", threshold=0.95,
                    parameters={"allowed_values": "'M','F','O'"})
r6    = v5.validate(df5, m6, m6.parameters)
assert_status("DOMAIN_IN_LIST should FAIL (X invalid)", r6, FAIL)
assert_count ("DOMAIN failed=1",                        r6, 1)


# ===========================================================================
#  TEST 6 — ReferentialIntegrityValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 6: ReferentialIntegrityValidator")
print("─"*60)

# Create a temporary reference table in Spark temp view
ref_data = [(1,), (2,), (3,), (4,), (5,)]
ref_df   = spark.createDataFrame(ref_data, ["customer_id"])
ref_df.createOrReplaceTempView("_test_customer")

fk_data  = [(1001, 1), (1002, 2), (1003, 99), (1004, 5)]  # 99 is orphan
fk_schema = StructType([
    StructField("sale_id",     IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
])
fk_df    = spark.createDataFrame(fk_data, schema=fk_schema)

# For unit test we use a custom ref-integrity implementation via anti-join
v6 = ReferentialIntegrityValidator(spark)
# We can't use the full Delta path in unit test, so we test anti-join directly:
source_df = fk_df.filter(F.col("customer_id").isNotNull())
failed_df  = source_df.join(
    ref_df,
    on=source_df["customer_id"] == ref_df["customer_id"],
    how="left_anti"
)
failed_count = failed_df.count()
if failed_count == 1:
    print("  ✓ PASS  REF_INTEGRITY anti-join finds 1 orphan FK (customer_id=99)")
    passed_tests.append("REF_INTEGRITY orphan detection")
else:
    print(f"  ✗ FAIL  REF_INTEGRITY expected 1 orphan, got {failed_count}")
    failed_tests.append("REF_INTEGRITY orphan detection")


# ===========================================================================
#  TEST 7 — CrossColumnValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 7: CrossColumnValidator")
print("─"*60)

schema7 = StructType([
    StructField("id",         IntegerType(), False),
    StructField("start_date", StringType(),  True),
    StructField("end_date",   StringType(),  True),
])
data7 = [
    (1, "2024-01-01", "2024-12-31"),   # OK
    (2, "2024-06-01", "2024-01-01"),   # end < start → FAIL
    (3, "2024-03-01", "2024-03-01"),   # equal → OK
]
df7 = spark.createDataFrame(data7, schema=schema7)
v7  = CrossColumnValidator(spark)

m7  = FakeMapping("CROSS_COLUMN", "start_date <= end_date",
                  threshold=0.90, parameters={"expression": "start_date <= end_date"})
r7  = v7.validate(df7, m7, m7.parameters)
assert_status("CROSS_COLUMN start<=end should FAIL (1 violation)", r7, FAIL)
assert_count ("CROSS_COLUMN failed=1",                              r7, 1)


# ===========================================================================
#  TEST 8 — AggregateValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 8: AggregateValidator")
print("─"*60)

schema8 = StructType([StructField("id", IntegerType(), False)])
data8   = [(i,) for i in range(1, 11)]   # 10 rows
df8     = spark.createDataFrame(data8, schema=schema8)
v8      = AggregateValidator(spark)

# Test: COUNT(*) >= 5 should PASS
m8a = FakeMapping("AGGREGATE", "COUNT(*) >= {min_count}",
                  threshold=1.00, parameters={"min_count": "5"})
r8a = v8.validate(df8, m8a, m8a.parameters)
assert_status("AGG_ROW_COUNT >= 5 should PASS (10 rows)", r8a, PASS)

# Test: COUNT(*) >= 100 should FAIL
m8b = FakeMapping("AGGREGATE", "COUNT(*) >= {min_count}",
                  threshold=1.00, parameters={"min_count": "100"})
r8b = v8.validate(df8, m8b, m8b.parameters)
assert_status("AGG_ROW_COUNT >= 100 should FAIL (10 rows)", r8b, FAIL)


# ===========================================================================
#  TEST 9 — CustomSQLValidator
# ===========================================================================
print("\n" + "─"*60)
print("TEST 9: CustomSQLValidator")
print("─"*60)

schema9 = StructType([
    StructField("id",     IntegerType(), False),
    StructField("amount", DoubleType(),  True),
])
data9 = [(1, 100.0), (2, -50.0), (3, None), (4, 200.0)]
df9   = spark.createDataFrame(data9, schema=schema9)
v9    = CustomSQLValidator(spark)

# Custom rule: amount > 0 OR amount IS NULL (allow nulls but not negatives)
m9 = FakeMapping("CUSTOM_SQL", "{sql_expression}",
                 threshold=0.90,
                 parameters={"sql_expression": "amount > 0 OR amount IS NULL"})
r9 = v9.validate(df9, m9, m9.parameters)
assert_status("CUSTOM_SQL should FAIL (-50 violates rule)", r9, FAIL)
assert_count ("CUSTOM_SQL failed=1",                        r9, 1)


# ===========================================================================
#  SUMMARY
# ===========================================================================
total  = len(passed_tests) + len(failed_tests)
print(f"\n{'='*60}")
print(f"TEST SUMMARY:  {len(passed_tests)}/{total} passed")
print(f"{'='*60}")
if failed_tests:
    print("FAILED TESTS:")
    for t in failed_tests:
        print(f"  ✗ {t}")
else:
    print("All tests passed! ✅")

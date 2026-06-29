"""
================================================================================
Enterprise Data Quality Framework
Step 2 – Seed Sample Data
================================================================================
Creates two sample Delta tables in  dq_framework.staging
    customer   – used for completeness, pattern, range, domain rules
    sales      – used for cross-table / referential integrity rules

Then inserts sample metadata rows into all config tables so that you can
run main.py immediately and see real DQ results.

Run AFTER 01_create_metadata_tables.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, DateType, TimestampType, BooleanType, LongType
)
from pyspark.sql.functions import current_timestamp, lit, to_date
import datetime

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# 1.  Create staging.customer (sample source table)
# ============================================================================
print("Creating staging.customer ...")

customer_data = [
    # id, name,          email,                   age,  salary,   gender, country, status,      phone
    (1,  "Alice Smith",  "alice@example.com",      30,   75000.0,  "F",   "US",    "ACTIVE",    "+1-555-0101"),
    (2,  "Bob Jones",    "bob.jones@example.com",  45,   92000.0,  "M",   "US",    "ACTIVE",    "+1-555-0102"),
    (3,  "Carol White",  None,                     28,   60000.0,  "F",   "IN",    "ACTIVE",    "+91-9876543210"),
    (4,  "Dave Brown",   "not-an-email",            -5,  120000.0, "M",   "UK",    "INACTIVE",  "123"),
    (5,  "Eve Davis",    "eve@example.com",         35,   85000.0,  "F",   "US",    "ACTIVE",    "+1-555-0105"),
    (6,  "Frank Lee",    "frank@example.com",       52,  200000.0, "M",   "CA",    "ACTIVE",    "+1-555-0106"),
    (7,  "Grace Kim",    "",                        29,   70000.0,  "F",   "US",    "ACTIVE",    "+1-555-0107"),
    (8,  "Harry Wilson", "harry@example.com",       200, -500.0,   "X",   "AU",    "ACTIVE",    "+61-412345678"),
    (9,  "Iris Chen",    "iris@example.com",        33,   88000.0,  "F",   "US",    "ACTIVE",    "+1-555-0109"),
    (10, "Jack Taylor",  "jack@example.com",        41,   95000.0,  "M",   "US",    "ACTIVE",    "+1-555-0110"),
    (10, "Jack Taylor",  "jack@example.com",        41,   95000.0,  "M",   "US",    "ACTIVE",    "+1-555-0110"),  # duplicate
    (11, "Karen Moore",  None,                      None, None,     None,  None,    None,         None),           # all nulls
]

customer_schema = StructType([
    StructField("customer_id",  IntegerType(), False),
    StructField("name",         StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("age",          IntegerType(), True),
    StructField("salary",       DoubleType(),  True),
    StructField("gender",       StringType(),  True),
    StructField("country",      StringType(),  True),
    StructField("status",       StringType(),  True),
    StructField("phone",        StringType(),  True),
])

customer_df = spark.createDataFrame(customer_data, schema=customer_schema)
customer_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dq_framework.staging.customer")

print(f"  ✓ staging.customer: {customer_df.count()} rows")

# ============================================================================
# 2.  Create staging.sales (used for cross-table ref-integrity)
# ============================================================================
print("Creating staging.sales ...")

from datetime import date

sales_data = [
    (1001, 1,  "2024-01-15", 250.0,  "USD"),
    (1002, 2,  "2024-01-16", 530.0,  "USD"),
    (1003, 99, "2024-01-17", 120.0,  "USD"),   # customer_id 99 does not exist → ref integrity failure
    (1004, 5,  "2024-01-18", 890.0,  "USD"),
    (1005, 6,  "2024-03-01", None,   "USD"),   # amount null
    (1006, 7,  "2024-03-15", -50.0,  "USD"),   # negative amount
    (1007, 9,  "2024-04-01", 1200.0, "USD"),
    (1008, 10, "2024-04-10", 330.0,  "USD"),
]

sales_schema = StructType([
    StructField("sale_id",     IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("sale_date",   StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  True),
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dq_framework.staging.sales")

print(f"  ✓ staging.sales: {sales_df.count()} rows")

# ============================================================================
# 3.  Seed config.config_sources
# ============================================================================
print("Seeding config.config_sources ...")
spark.sql("""
INSERT INTO dq_framework.config.config_sources VALUES
(1, 'CRM System',      'dq_framework', 'staging', 'Customer master data from CRM', 'Data Engineering', true, current_timestamp(), current_timestamp()),
(2, 'Sales Platform',  'dq_framework', 'staging', 'Sales transactions',           'Data Engineering', true, current_timestamp(), current_timestamp())
""")

# ============================================================================
# 4.  Seed config.config_tables
# ============================================================================
print("Seeding config.config_tables ...")
spark.sql("""
INSERT INTO dq_framework.config.config_tables VALUES
(1, 1, 'dq_framework', 'staging', 'customer', 'customer_id', NULL,        true, current_timestamp(), current_timestamp()),
(2, 2, 'dq_framework', 'staging', 'sales',    'sale_id',     'sale_date', true, current_timestamp(), current_timestamp())
""")

# ============================================================================
# 5.  Seed config.config_rules  (master rule library)
# ============================================================================
print("Seeding config.config_rules ...")
spark.sql("""
INSERT INTO dq_framework.config.config_rules VALUES
-- Completeness
(1,  'NULL_CHECK',       'COMPLETENESS',     '{col} IS NOT NULL',                             'HIGH',   0.95, 'Column value must not be NULL',                 1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(2,  'EMPTY_STRING',     'COMPLETENESS',     '{col} IS NOT NULL AND trim({col}) != ""',       'MEDIUM', 0.95, 'Column must not be empty or blank',             1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Uniqueness
(3,  'UNIQUE_CHECK',     'UNIQUENESS',       '{col}',                                         'HIGH',   1.00, 'Column values must be unique',                  1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(4,  'COMPOSITE_UNIQUE', 'UNIQUENESS',       '{col}',                                         'HIGH',   1.00, 'Composite key must be unique',                  1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Pattern
(5,  'EMAIL_REGEX',      'PATTERN',          '{col} RLIKE "^[a-zA-Z0-9._%+\\\\-]+@[a-zA-Z0-9.\\\\-]+\\\\.[a-zA-Z]{2,}$"', 'HIGH', 0.95, 'Email format validation', 1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(6,  'PHONE_REGEX',      'PATTERN',          '{col} RLIKE "^\\\\+?[1-9]\\\\d{1,14}$"',        'MEDIUM', 0.90, 'Phone number format (E.164)',                   1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(7,  'CUSTOM_REGEX',     'PATTERN',          '{col} RLIKE "{pattern}"',                       'MEDIUM', 0.95, 'Custom regex validation via parameter',          1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Range
(8,  'NUMERIC_BETWEEN',  'RANGE',            '{col} BETWEEN {min_val} AND {max_val}',         'HIGH',   0.95, 'Numeric value must be within range',             1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(9,  'GREATER_THAN',     'RANGE',            '{col} > {min_val}',                             'HIGH',   0.95, 'Value must be greater than threshold',           1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(10, 'NOT_NEGATIVE',     'RANGE',            '{col} >= 0',                                    'HIGH',   0.95, 'Value must be non-negative',                    1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Domain
(11, 'DOMAIN_IN_LIST',   'DOMAIN',           '{col} IN ({allowed_values})',                   'HIGH',   0.95, 'Value must be in allowed domain list',           1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Referential Integrity
(12, 'REF_INTEGRITY',    'REF_INTEGRITY',    'EXISTS_IN:{ref_catalog}.{ref_schema}.{ref_table}.{ref_column}', 'HIGH', 1.00, 'FK must exist in reference table', 1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Cross Column
(13, 'CROSS_COLUMN_EXPR','CROSS_COLUMN',     '{expression}',                                  'HIGH',   0.95, 'Cross-column validation expression',             1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Aggregate
(14, 'AGG_ROW_COUNT',    'AGGREGATE',        'COUNT(*) >= {min_count}',                       'HIGH',   1.00, 'Table must have minimum row count',              1, true, 'system', current_timestamp(), NULL, current_timestamp()),
(15, 'AGG_SUM_CHECK',    'AGGREGATE',        'SUM({col}) >= {min_sum}',                       'HIGH',   1.00, 'Aggregate sum must meet threshold',              1, true, 'system', current_timestamp(), NULL, current_timestamp()),

-- Custom SQL
(16, 'CUSTOM_SQL',       'CUSTOM_SQL',       '{sql_expression}',                              'HIGH',   0.95, 'Fully custom SQL expression (returns boolean)',  1, true, 'system', current_timestamp(), NULL, current_timestamp())
""")

# ============================================================================
# 6.  Seed config.rule_mapping  (bind rules to customer & sales tables)
# ============================================================================
print("Seeding config.rule_mapping ...")
spark.sql("""
INSERT INTO dq_framework.config.rule_mapping VALUES
-- customer table rules
(1,  1,  1, 'email',       NULL,             NULL, 'HIGH',   0.95, 10,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- NULL_CHECK  on email
(2,  2,  1, 'email',       NULL,             NULL, 'MEDIUM', 0.95, 20,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- EMPTY_STRING on email
(3,  5,  1, 'email',       NULL,             NULL, 'HIGH',   0.90, 30,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- EMAIL_REGEX  on email
(4,  1,  1, 'name',        NULL,             NULL, 'HIGH',   0.95, 40,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- NULL_CHECK  on name
(5,  3,  1, 'customer_id', NULL,             NULL, 'HIGH',   1.00, 50,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- UNIQUE_CHECK on customer_id
(6,  8,  1, 'age',         NULL,             NULL, 'HIGH',   0.95, 60,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- NUMERIC_BETWEEN age
(7,  9,  1, 'salary',      NULL,             NULL, 'HIGH',   0.95, 70,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- GREATER_THAN salary
(8,  11, 1, 'gender',      NULL,             NULL, 'HIGH',   0.95, 80,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- DOMAIN_IN_LIST gender
(9,  6,  1, 'phone',       NULL,             NULL, 'MEDIUM', 0.90, 90,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- PHONE_REGEX
(10, 14, 1, NULL,          NULL,             NULL, 'HIGH',   1.00, 100, true, 'system', current_timestamp(), NULL, current_timestamp()),  -- AGG_ROW_COUNT customer

-- sales table rules
(11, 1,  2, 'amount',      NULL,             NULL, 'HIGH',   0.95, 10,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- NULL_CHECK amount
(12, 10, 2, 'amount',      NULL,             NULL, 'HIGH',   0.95, 20,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- NOT_NEGATIVE amount
(13, 12, 2, 'customer_id', NULL,             NULL, 'HIGH',   1.00, 30,  true, 'system', current_timestamp(), NULL, current_timestamp()),  -- REF_INTEGRITY customer_id
(14, 16, 2, NULL,          NULL,             NULL, 'HIGH',   0.95, 40,  true, 'system', current_timestamp(), NULL, current_timestamp())   -- CUSTOM_SQL
""")

# ============================================================================
# 7.  Seed config.rule_parameters
# ============================================================================
print("Seeding config.rule_parameters ...")
spark.sql("""
INSERT INTO dq_framework.config.rule_parameters VALUES
-- mapping_id=6 → NUMERIC_BETWEEN age → 0 to 120
(1, 6, 'min_val', '0',   current_timestamp()),
(2, 6, 'max_val', '120', current_timestamp()),

-- mapping_id=7 → GREATER_THAN salary → > 0
(3, 7, 'min_val', '0', current_timestamp()),

-- mapping_id=8 → DOMAIN_IN_LIST gender → M, F, O
(4, 8, 'allowed_values', "'M','F','O'", current_timestamp()),

-- mapping_id=10 → AGG_ROW_COUNT customer → >= 5
(5, 10, 'min_count', '5', current_timestamp()),

-- mapping_id=13 → REF_INTEGRITY customer_id in sales → must exist in customer
(6, 13, 'ref_catalog', 'dq_framework',  current_timestamp()),
(7, 13, 'ref_schema',  'staging',        current_timestamp()),
(8, 13, 'ref_table',   'customer',       current_timestamp()),
(9, 13, 'ref_column',  'customer_id',    current_timestamp()),

-- mapping_id=14 → CUSTOM_SQL on sales: amount > 0 OR currency IS NOT NULL
(10, 14, 'sql_expression', 'amount > 0 OR currency IS NOT NULL', current_timestamp())
""")

print()
print("=" * 60)
print("✅  Sample data seeding complete!")
print()
print("Source tables:")
print("   dq_framework.staging.customer  –", spark.table("dq_framework.staging.customer").count(), "rows")
print("   dq_framework.staging.sales     –", spark.table("dq_framework.staging.sales").count(), "rows")
print()
print("Metadata rows:")
for t, q in [
    ("config_sources",  "SELECT COUNT(*) FROM dq_framework.config.config_sources"),
    ("config_tables",   "SELECT COUNT(*) FROM dq_framework.config.config_tables"),
    ("config_rules",    "SELECT COUNT(*) FROM dq_framework.config.config_rules"),
    ("rule_mapping",    "SELECT COUNT(*) FROM dq_framework.config.rule_mapping"),
    ("rule_parameters", "SELECT COUNT(*) FROM dq_framework.config.rule_parameters"),
]:
    cnt = spark.sql(q).collect()[0][0]
    print(f"   {t}: {cnt} rows")

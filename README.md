# Enterprise Metadata-Driven Data Quality Framework
### Databricks + PySpark + Delta Lake + Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-Ready-red?logo=databricks)](https://databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?logo=apache-spark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-blue)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python)](https://python.org)

---

## Overview

A **production-ready, metadata-driven Data Quality engine** that runs entirely on **Databricks**. All rules, joins, filters, and configurations live in **Delta Tables** — zero code changes needed to add new DQ rules.

## ⚡ Quick Start (Databricks)

1. Clone this repo into **Databricks Repos**
2. Open `DQ_Framework_Complete_Notebook.py` as a notebook
3. Run **all cells top to bottom** — it bootstraps everything automatically

## 📁 Project Structure

```
databricks-dq-framework/
│
├── DQ_Framework_Complete_Notebook.py   ← ⭐ MAIN: Single notebook, run this!
│
├── dq_framework/                       ← Modular source code
│   ├── 00_setup/
│   │   ├── 01_create_metadata_tables.py
│   │   └── 02_seed_sample_data.py
│   ├── config/
│   │   └── framework_config.py
│   ├── metadata/
│   │   └── metadata_loader.py
│   ├── readers/
│   │   └── delta_reader.py
│   ├── joins/
│   │   └── join_engine.py
│   ├── filters/
│   │   └── filter_engine.py
│   ├── validators/
│   │   ├── base_validator.py
│   │   └── all_validators.py
│   ├── engine/
│   │   └── rule_engine.py
│   ├── orchestrator/
│   │   └── dq_orchestrator.py
│   ├── scoring/
│   │   └── dq_score_engine.py
│   ├── audit/
│   │   └── audit_logger.py
│   ├── failed_records/
│   │   └── failed_records_writer.py
│   ├── alerts/
│   │   └── alert_manager.py
│   └── tests/
│       └── test_all_validators.py
```

## 🧩 Supported Rule Types

| Rule Type | Description | Example |
|-----------|-------------|---------|
| `COMPLETENESS` | Null / blank checks | `email IS NOT NULL` |
| `UNIQUENESS` | Duplicate / composite key | Window dedup |
| `PATTERN` | Regex validation | Email, Phone, PAN |
| `RANGE` | Numeric / date range | `age BETWEEN 0 AND 120` |
| `DOMAIN` | Allowed value list | `gender IN ('M','F','O')` |
| `REF_INTEGRITY` | FK lookup (anti-join) | `customer_id` exists in customer |
| `CROSS_COLUMN` | Multi-column expressions | `start_date <= end_date` |
| `AGGREGATE` | Table-level aggregates | `COUNT(*) >= 100` |
| `CUSTOM_SQL` | Any boolean SQL | `amount > 0 OR currency IS NOT NULL` |

## 📦 Metadata Delta Tables

All configuration lives in `dq_framework` catalog:

| Table | Purpose |
|-------|---------|
| `config.config_sources` | Registered source systems |
| `config.config_tables` | Tables registered for DQ |
| `config.config_rules` | Master rule library |
| `config.rule_mapping` | Rule → Table+Column binding |
| `config.rule_parameters` | Dynamic parameter overrides |
| `config.join_config` | Multi-hop join chains |
| `results.execution_history` | Per-rule execution results |
| `results.failed_records` | Rows that failed a rule |
| `results.audit_logs` | Operational audit trail |
| `results.dq_score` | Aggregated DQ % scores |

## ➕ Adding a New Rule (Zero Code Change)

```sql
-- 1. Add to master rule library
INSERT INTO dq_framework.config.config_rules VALUES
(17, 'SSN_REGEX', 'PATTERN',
 '{col} RLIKE "^\\d{3}-\\d{2}-\\d{4}$"',
 'HIGH', 0.95, 'SSN format', 1, true,
 'admin', current_timestamp(), NULL, current_timestamp());

-- 2. Bind to a table+column
INSERT INTO dq_framework.config.rule_mapping VALUES
(15, 17, 1, 'ssn', NULL, NULL, 'HIGH', 0.95, 110,
 true, 'admin', current_timestamp(), NULL, current_timestamp());

-- Re-run the notebook — new rule executes automatically ✅
```

## 🏗️ Design Patterns

- **Factory Pattern** — `RuleEngine` picks the right validator from `VALIDATOR_REGISTRY`
- **Strategy Pattern** — All validators implement the same `validate()` interface
- **Metadata-Driven** — Zero code changes needed for new rules

## 📊 Output

After running the notebook:
- `dq_framework.results.execution_history` — PASS/FAIL per rule
- `dq_framework.results.failed_records` — Exact rows that failed
- `dq_framework.results.dq_score` — Overall DQ % score per table

## 🚀 Databricks Deployment

```bash
# Clone in Databricks Repos
Settings → Linked Git provider → Add repo → https://github.com/shashvindu/databricks-dq-framework

# Or via Databricks CLI
databricks repos create --url https://github.com/shashvindu/databricks-dq-framework --provider gitHub
```

## ⚙️ Tech Stack

- **Databricks Runtime** 13.x / 14.x / 15.x
- **Unity Catalog**
- **Delta Lake 3.x**
- **PySpark 3.x**
- **Python 3.9+**

---
*Built by shashvindu — Enterprise Data Quality on Databricks*

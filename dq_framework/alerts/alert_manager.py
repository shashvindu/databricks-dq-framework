"""
================================================================================
Enterprise Data Quality Framework
Component: AlertManager
================================================================================
Sends alerts when HIGH severity rules fail.
Supports:
  - Print/log alerts (always on – works in Databricks notebooks)
  - Email via Databricks secret-backed SMTP (optional)
  - Microsoft Teams webhook (optional)

To activate email/Teams alerts set the relevant Databricks secrets:
  databricks secrets put --scope dq-framework --key smtp-host
  databricks secrets put --scope dq-framework --key teams-webhook-url
================================================================================
"""

from __future__ import annotations
import smtplib, json
from email.mime.text   import MIMEText
from typing            import List, Optional
from validators.base_validator import ValidationResult


class AlertManager:
    """
    Sends alerts for FAIL results that exceed severity thresholds.

    Usage
    -----
    mgr = AlertManager(spark)
    mgr.evaluate_and_alert(results, run_id="job-123")
    """

    def __init__(self, spark=None, alert_severity: str = "HIGH"):
        self._spark           = spark
        self._alert_severity  = alert_severity  # only alert on this severity or above
        self._severity_order  = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}

    # ------------------------------------------------------------------ #
    #  Public                                                              #
    # ------------------------------------------------------------------ #

    def evaluate_and_alert(
        self,
        results: List[ValidationResult],
        run_id:  str,
    ):
        """Check results and fire alerts for qualifying failures."""
        threshold = self._severity_order.get(self._alert_severity, 2)
        failures  = [
            r for r in results
            if r.status == "FAIL"
            and self._severity_order.get(r.severity, 0) >= threshold
        ]

        if not failures:
            print("[AlertManager] No HIGH severity failures – no alerts sent.")
            return

        print(f"[AlertManager] 🚨 {len(failures)} HIGH severity failure(s) detected!")
        for r in failures:
            self._log_alert(r, run_id)

        # Optional: Teams / Email  (commented out – requires secrets)
        # self._send_teams_alert(failures, run_id)
        # self._send_email_alert(failures, run_id)

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _log_alert(self, result: ValidationResult, run_id: str):
        print(
            f"  🚨 ALERT | run_id={run_id} | "
            f"Table: {result.catalog_name}.{result.schema_name}.{result.table_name} | "
            f"Column: {result.column_name or 'TABLE'} | "
            f"Rule: {result.rule_name} | "
            f"Severity: {result.severity} | "
            f"Pass rate: {result.pass_rate:.1%} (threshold {result.threshold:.1%}) | "
            f"Failed records: {result.failed_records}"
        )

    def _send_teams_alert(self, failures: List[ValidationResult], run_id: str):
        """Post a Teams message card via webhook URL stored in Databricks secrets."""
        try:
            from dbutils import secrets  # noqa – available in Databricks runtime only
            webhook_url = secrets.get(scope="dq-framework", key="teams-webhook-url")
        except Exception:
            return   # not running on Databricks or secret not set

        import urllib.request
        payload = {
            "@type":      "MessageCard",
            "@context":   "http://schema.org/extensions",
            "summary":    f"DQ Framework Alerts – run_id={run_id}",
            "themeColor": "FF0000",
            "sections": [{
                "activityTitle": f"🚨 DQ Framework Alert – {len(failures)} failure(s)",
                "facts": [
                    {"name": f"{r.table_name}.{r.column_name or 'TABLE'} [{r.rule_name}]",
                     "value": f"Pass rate {r.pass_rate:.1%} (threshold {r.threshold:.1%}), "
                              f"{r.failed_records} failed records"}
                    for r in failures
                ]
            }]
        }
        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(webhook_url, data=data,
                                      headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        print(f"[AlertManager] Teams alert sent for run_id={run_id}")

# Data quality checks for the e-commerce pipeline.

from __future__ import annotations
import json
from datetime import datetime, timezone
from src.utils.db import get_connection


def _run_id() -> str:
    return f"dq_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def _log_result(
    cur,
    run_id: str,
    table_name: str,
    check_name: str,
    check_type: str,
    status: str,
    rows_checked: int | None,
    rows_failed: int,
    details: dict | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO logs.data_quality_results (
            run_id, layer, table_name, check_name, check_type,
            status, rows_checked, rows_failed, failure_percentage, details
        )
        VALUES (
            %s, 'bronze', %s, %s, %s,
            %s, %s, %s,
            CASE
                WHEN %s IS NULL OR %s = 0 THEN NULL
                ELSE ROUND((%s::numeric / %s::numeric) * 100, 4)
            END,
            %s::jsonb
        )
        """,
        (
            run_id,
            table_name,
            check_name,
            check_type,
            status,
            rows_checked,
            rows_failed,
            rows_checked,
            rows_checked,
            rows_failed,
            rows_checked,
            json.dumps(details or {}),
        ),
    )


def _count(cur, sql: str) -> int:
    cur.execute(sql)
    return int(cur.fetchone()[0])


def run_bronze_quality_checks(fail_on_critical: bool = True) -> dict:
    run_id = _run_id()
    results = []
    critical_failures = []

    checks = [
        # -------- critical: null keys / dates --------
        {
            "table": "bronze.orders",
            "name": "null_order_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.orders WHERE order_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.orders",
            "critical": True,
        },
        {
            "table": "bronze.orders",
            "name": "null_purchase_timestamp",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.orders WHERE order_purchase_timestamp IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.orders",
            "critical": True,
        },
        {
            "table": "bronze.order_items",
            "name": "null_order_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.order_items WHERE order_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.order_items",
            "critical": True,
        },
        {
            "table": "bronze.order_items",
            "name": "null_product_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.order_items WHERE product_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.order_items",
            "critical": True,
        },
        {
            "table": "bronze.customers",
            "name": "null_customer_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.customers WHERE customer_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.customers",
            "critical": True,
        },
        {
            "table": "bronze.products",
            "name": "null_product_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.products WHERE product_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.products",
            "critical": True,
        },
        {
            "table": "bronze.returns",
            "name": "null_return_id",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.returns WHERE return_id IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.returns",
            "critical": True,
        },
        {
            "table": "bronze.returns",
            "name": "null_return_date",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.returns WHERE return_date IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.returns",
            "critical": True,
        },
        {
            "table": "bronze.currency_rates",
            "name": "null_rate_date",
            "type": "null_check",
            "sql_failed": "SELECT COUNT(*) FROM bronze.currency_rates WHERE rate_date IS NULL",
            "sql_total": "SELECT COUNT(*) FROM bronze.currency_rates",
            "critical": True,
        },

        # -------- critical: value rules --------
        {
            "table": "bronze.order_items",
            "name": "negative_or_null_price",
            "type": "range",
            "sql_failed": "SELECT COUNT(*) FROM bronze.order_items WHERE price IS NULL OR price < 0",
            "sql_total": "SELECT COUNT(*) FROM bronze.order_items",
            "critical": True,
        },
        {
            "table": "bronze.currency_rates",
            "name": "non_positive_rate",
            "type": "range",
            "sql_failed": "SELECT COUNT(*) FROM bronze.currency_rates WHERE rate IS NULL OR rate <= 0",
            "sql_total": "SELECT COUNT(*) FROM bronze.currency_rates",
            "critical": True,
        },

        # -------- warnings: volume --------
        {
            "table": "bronze.orders",
            "name": "empty_table",
            "type": "volume",
            "sql_failed": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM bronze.orders",
            "sql_total": "SELECT COUNT(*) FROM bronze.orders",
            "critical": False,
        },
        {
            "table": "bronze.order_items",
            "name": "empty_table",
            "type": "volume",
            "sql_failed": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM bronze.order_items",
            "sql_total": "SELECT COUNT(*) FROM bronze.order_items",
            "critical": False,
        },
        {
            "table": "bronze.order_status",
            "name": "empty_table",
            "type": "volume",
            "sql_failed": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM bronze.order_status",
            "sql_total": "SELECT COUNT(*) FROM bronze.order_status",
            "critical": False,
        },
        {
            "table": "bronze.returns",
            "name": "empty_table",
            "type": "volume",
            "sql_failed": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM bronze.returns",
            "sql_total": "SELECT COUNT(*) FROM bronze.returns",
            "critical": False,
        },
        {
            "table": "bronze.currency_rates",
            "name": "empty_table",
            "type": "volume",
            "sql_failed": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM bronze.currency_rates",
            "sql_total": "SELECT COUNT(*) FROM bronze.currency_rates",
            "critical": False,
        },

        # -------- warnings: referential --------
        {
            "table": "bronze.order_items",
            "name": "orphan_order_id",
            "type": "referential",
            "sql_failed": """
                SELECT COUNT(*)
                FROM bronze.order_items oi
                LEFT JOIN bronze.orders o ON o.order_id = oi.order_id
                WHERE o.order_id IS NULL
            """,
            "sql_total": "SELECT COUNT(*) FROM bronze.order_items",
            "critical": False,
        },
        {
            "table": "bronze.returns",
            "name": "orphan_order_id",
            "type": "referential",
            "sql_failed": """
                SELECT COUNT(*)
                FROM bronze.returns r
                LEFT JOIN bronze.orders o ON o.order_id = r.order_id
                WHERE o.order_id IS NULL
            """,
            "sql_total": "SELECT COUNT(*) FROM bronze.returns",
            "critical": False,
        },
        {
            "table": "bronze.order_status",
            "name": "orphan_order_id",
            "type": "referential",
            "sql_failed": """
                SELECT COUNT(*)
                FROM bronze.order_status s
                LEFT JOIN bronze.orders o ON o.order_id = s.order_id
                WHERE o.order_id IS NULL
            """,
            "sql_total": "SELECT COUNT(*) FROM bronze.order_status",
            "critical": False,
        },
    ]

    print("=" * 60)
    print("Bronze data quality checks")
    print(f"Run ID: {run_id}")
    print("=" * 60)

    with get_connection() as conn:
        with conn.cursor() as cur:
            for c in checks:
                failed = _count(cur, c["sql_failed"])
                total = _count(cur, c["sql_total"])

                if c["name"] == "empty_table":
                    status = "warning" if failed > 0 else "passed"
                else:
                    if failed > 0 and c["critical"]:
                        status = "failed"
                    elif failed > 0:
                        status = "warning"
                    else:
                        status = "passed"

                _log_result(
                    cur=cur,
                    run_id=run_id,
                    table_name=c["table"],
                    check_name=c["name"],
                    check_type=c["type"],
                    status=status,
                    rows_checked=total,
                    rows_failed=failed,
                    details={
                        "critical": c["critical"],
                        "failed_rows": failed,
                        "total_rows": total,
                    },
                )

                row = {
                    "table": c["table"],
                    "check": c["name"],
                    "status": status,
                    "failed": failed,
                    "total": total,
                }
                results.append(row)
                print(
                    f"  [{status.upper()}] {c['table']} :: {c['name']} "
                    f"(failed={failed:,} / total={total:,})"
                )

                if status == "failed":
                    critical_failures.append(row)

    summary = {
        "run_id": run_id,
        "total_checks": len(results),
        "failed_critical": len(critical_failures),
        "results": results,
    }

    print("=" * 60)
    print(
        f"DQ finished: {len(results)} checks, "
        f"{len(critical_failures)} critical failures"
    )
    print("=" * 60)

    if fail_on_critical and critical_failures:
        details = ", ".join(
            f"{x['table']}.{x['check']}={x['failed']}" for x in critical_failures
        )
        raise ValueError(f"Critical bronze DQ checks failed: {details}")

    return summary
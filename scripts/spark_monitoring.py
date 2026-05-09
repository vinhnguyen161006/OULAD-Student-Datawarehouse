"""Spark monitoring job — health checks cho DWH + Mart, ghi vào monitoring_log."""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MYSQL_HOST     = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT     = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpassword")

DWH_DB    = "student_dwh"
MART_DB   = "student_data_mart"
LOG_TABLE = "monitoring_log"

JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"


def jdbc_url(database: str) -> str:
    return (
        f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{database}"
        "?useSSL=false"
        "&allowPublicKeyRetrieval=true"
        "&serverTimezone=UTC"
    )


# ---------------------------------------------------------------------------
# Check helpers
# ---------------------------------------------------------------------------

CheckResult = Tuple[str, str, str]  # (check_name, status, detail)

# Ngưỡng (relax-able)
SCORE_NULL_WARN_PCT = 25.0   # > 25% NULL score → WARN
SCORE_NULL_FAIL_PCT = 60.0   # > 60% NULL score → FAIL
VALID_FINAL_RESULTS = {"Pass", "Fail", "Withdrawn", "Distinction"}
VALID_RISK_GROUPS   = {"Low", "Medium", "High", "Unknown"}


def _read_table(spark: SparkSession, database: str, table: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url(database))
        .option("driver", JDBC_DRIVER)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("dbtable", table)
        .load()
    )


def check_row_count(spark, database, table, *, min_rows=1, exact=None) -> CheckResult:
    name = f"{database}.{table}.row_count"
    try:
        n = _read_table(spark, database, table).count()
        if exact is not None:
            ok = n == exact
            return (name, "PASS" if ok else "FAIL", f"rows={n} expected={exact}")
        ok = n >= min_rows
        return (name, "PASS" if ok else "FAIL", f"rows={n} min={min_rows}")
    except Exception as e:
        return (name, "ERROR", str(e)[:500])


def check_null_rate(spark, database, table, column, *, warn_pct, fail_pct) -> CheckResult:
    name = f"{database}.{table}.{column}.null_rate"
    try:
        df = _read_table(spark, database, table)
        total = df.count()
        if total == 0:
            return (name, "WARN", "table empty")
        nulls = df.filter(F.col(column).isNull()).count()
        pct = nulls * 100.0 / total
        if pct > fail_pct:
            status = "FAIL"
        elif pct > warn_pct:
            status = "WARN"
        else:
            status = "PASS"
        return (name, status, f"null={nulls}/{total} ({pct:.2f}%)")
    except Exception as e:
        return (name, "ERROR", str(e)[:500])


def check_accepted_values(spark, database, table, column, accepted) -> CheckResult:
    name = f"{database}.{table}.{column}.accepted_values"
    try:
        df = _read_table(spark, database, table)
        bad = (
            df.filter(F.col(column).isNotNull())
              .filter(~F.col(column).isin(*accepted))
              .select(column)
              .distinct()
              .limit(20)
              .collect()
        )
        if not bad:
            return (name, "PASS", f"all values in {sorted(accepted)}")
        offending = sorted({r[column] for r in bad})
        return (name, "FAIL", f"unexpected={offending}")
    except Exception as e:
        return (name, "ERROR", str(e)[:500])


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_checks(spark: SparkSession) -> List[CheckResult]:
    checks: List[CheckResult] = []

    # DWH — row counts
    checks.append(check_row_count(spark, DWH_DB, "Dim_Time", exact=4))
    for table in ("Dim_Student", "Dim_Course", "Dim_Assessment", "Fact_Performance"):
        checks.append(check_row_count(spark, DWH_DB, table))

    # DWH — null rates trên Fact_Performance
    for col in ("student_key", "course_key", "assessment_key", "time_key"):
        checks.append(
            check_null_rate(spark, DWH_DB, "Fact_Performance", col, warn_pct=0.0, fail_pct=0.0)
        )
    checks.append(
        check_null_rate(
            spark, DWH_DB, "Fact_Performance", "score",
            warn_pct=SCORE_NULL_WARN_PCT, fail_pct=SCORE_NULL_FAIL_PCT,
        )
    )

    # DWH — accepted values
    checks.append(
        check_accepted_values(spark, DWH_DB, "Fact_Performance", "final_result", VALID_FINAL_RESULTS)
    )
    checks.append(
        check_accepted_values(spark, DWH_DB, "Fact_Performance", "risk_group", VALID_RISK_GROUPS)
    )

    # Mart — mỗi mart phải có ít nhất 1 dòng
    for mart in (
        "stg_fact_performance",
        "mart_result_by_module",
        "mart_score_by_gender",
        "mart_education_impact",
        "mart_vle_engagement",
        "mart_at_risk_students",
    ):
        checks.append(check_row_count(spark, MART_DB, mart))

    return checks


def write_log(spark: SparkSession, results: List[CheckResult]) -> None:
    now = datetime.utcnow()
    rows = [(name, status, detail, now) for (name, status, detail) in results]
    schema = StructType([
        StructField("check_name", StringType(), False),
        StructField("status",     StringType(), False),
        StructField("detail",     StringType(), True),
        StructField("checked_at", TimestampType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema)

    (
        df.write.format("jdbc")
        .option("url", jdbc_url(DWH_DB))
        .option("driver", JDBC_DRIVER)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("dbtable", LOG_TABLE)
        .mode("append")
        .save()
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("oulad_monitoring")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        results = run_checks(spark)

        for name, status, detail in results:
            print(f"[{status:5s}] {name} — {detail}")

        write_log(spark, results)

        n_fail  = sum(1 for _, s, _ in results if s == "FAIL")
        n_error = sum(1 for _, s, _ in results if s == "ERROR")
        n_warn  = sum(1 for _, s, _ in results if s == "WARN")
        n_pass  = sum(1 for _, s, _ in results if s == "PASS")

        print(
            f"\nSummary: PASS={n_pass} WARN={n_warn} FAIL={n_fail} ERROR={n_error}"
            f" / total={len(results)}"
        )

        if n_fail or n_error:
            raise SystemExit(
                f"Monitoring failed: {n_fail} FAIL + {n_error} ERROR"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

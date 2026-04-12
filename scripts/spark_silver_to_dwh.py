"""
spark_silver_to_dwh.py
=======================
DAG 03 — Đọc Silver Parquet từ MinIO → build Star Schema → load MySQL student_dwh.

Silver (s3a://oulad-silver/) → MySQL student_dwh (Dim_* + Fact_Performance)

Thứ tự load (bắt buộc theo FK dependency):
  Dim_Time      — đã có từ init.sql, chỉ đọc để lấy time_key
  Dim_Course    ← silver/student_info/
  Dim_Student   ← silver/student_info/
  Dim_Assessment← silver/assessments/
  Fact_Performance ← student_info + student_assessment + assessments + vle_clicks

Metrics tính trong script:
  avg_score      = SUM(score * weight) / SUM(weight)
  num_submissions= COUNT(submissions có score)
  score_vs_avg   = avg_score - AVG(avg_score) OVER (PARTITION BY code_presentation)
  risk_group     = Low (>=70) / Medium (50-69) / High (<50 hoặc NULL)

Biến môi trường (inject từ Airflow):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    MINIO_BUCKET_SILVER
    SPARK_MASTER
"""

import os
import logging

import mysql.connector
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MYSQL_HOST          = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT          = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER          = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD      = os.getenv("MYSQL_PASSWORD", "rootpassword")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "oulad-silver")
SPARK_MASTER        = os.getenv("SPARK_MASTER", "local[*]")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/student_dwh"
    "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
)
JDBC_PROPS = {
    "user":     MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver":   "com.mysql.cj.jdbc.Driver",
}
WRITE_PROPS = {**JDBC_PROPS, "batchsize": "5000"}


def silver_path(table: str) -> str:
    return f"s3a://{MINIO_BUCKET_SILVER}/{table}/"


# ── Raw SQL helper ────────────────────────────────────────────────────────────

def run_sql(statements: list) -> None:
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=int(MYSQL_PORT),
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database="student_dwh", connection_timeout=30,
    )
    cursor = conn.cursor()
    try:
        for stmt in statements:
            log.info(f"[SQL  ] {stmt}")
            cursor.execute(stmt)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def truncate_dwh_tables() -> None:
    run_sql([
        "SET FOREIGN_KEY_CHECKS = 0",
        "TRUNCATE TABLE Fact_Performance",
        "TRUNCATE TABLE Dim_Assessment",
        "TRUNCATE TABLE Dim_Student",
        "TRUNCATE TABLE Dim_Course",
        "SET FOREIGN_KEY_CHECKS = 1",
    ])
    log.info("[TRUNC] Fact_Performance, Dim_Assessment, Dim_Student, Dim_Course")


# ── Dim builders ──────────────────────────────────────────────────────────────

def build_dim_course(spark: SparkSession) -> DataFrame:
    log.info("[BUILD] Dim_Course ← silver/courses/")
    df = spark.read.parquet(silver_path("courses"))
    window = Window.orderBy("code_module", "code_presentation")
    dim = (
        df.select("code_module", "code_presentation", "module_presentation_length")
        .dropDuplicates(["code_module", "code_presentation"])
        .withColumn("course_key", F.row_number().over(window))
    )
    dim.cache()
    dim.write.jdbc(url=JDBC_URL, table="Dim_Course", mode="append", properties=WRITE_PROPS)
    log.info(f"[DONE ] Dim_Course: {dim.count()} rows")
    return dim


def build_dim_student(spark: SparkSession) -> DataFrame:
    log.info("[BUILD] Dim_Student ← silver/student_info/")
    df = spark.read.parquet(silver_path("student_info"))
    window = Window.orderBy("id_student")
    dim = (
        df.select(
            "id_student", "gender", "region", "highest_education",
            "imd_band", "age_band", "disability",
            "num_of_prev_attempts", "studied_credits",
        )
        .dropDuplicates(["id_student"])
        .withColumn("student_key", F.row_number().over(window))
    )
    dim.cache()
    dim.write.jdbc(url=JDBC_URL, table="Dim_Student", mode="append", properties=WRITE_PROPS)
    log.info(f"[DONE ] Dim_Student: {dim.count():,} rows")
    return dim


def build_dim_assessment(spark: SparkSession) -> DataFrame:
    log.info("[BUILD] Dim_Assessment ← silver/assessments/")
    df = spark.read.parquet(silver_path("assessments"))
    window = Window.orderBy("id_assessment")
    dim = (
        df.select("id_assessment", "code_module", "assessment_type", "weight",
                  F.col("date").alias("day_due"))
        .dropDuplicates(["id_assessment"])
        .withColumn("assessment_key", F.row_number().over(window))
    )
    dim.cache()
    dim.write.jdbc(url=JDBC_URL, table="Dim_Assessment", mode="append", properties=WRITE_PROPS)
    log.info(f"[DONE ] Dim_Assessment: {dim.count()} rows")
    return dim


# ── Fact builder ──────────────────────────────────────────────────────────────

def build_fact_performance(
    spark: SparkSession,
    dim_student: DataFrame,
    dim_course: DataFrame,
    dim_time: DataFrame,
) -> None:
    log.info("[BUILD] Fact_Performance")

    base = spark.read.parquet(silver_path("student_info")).select(
        "id_student", "code_module", "code_presentation", "final_result"
    )

    sa = spark.read.parquet(silver_path("student_assessment")).filter(
        F.col("score").isNotNull()
    )
    assessments = spark.read.parquet(silver_path("assessments")).select(
        "id_assessment", "code_module", "code_presentation", "weight"
    )
    sa_meta = sa.join(assessments, "id_assessment")

    score_agg = (
        sa_meta
        .groupBy("id_student", "code_module", "code_presentation")
        .agg(
            (F.sum(F.col("score") * F.col("weight")) / F.sum("weight")).alias("avg_score"),
            F.count("*").cast("int").alias("num_submissions"),
        )
    )

    clicks = spark.read.parquet(silver_path("vle_clicks")).select(
        "id_student", "code_module", "code_presentation", "total_clicks"
    )

    fact_base = (
        base
        .join(score_agg, ["id_student", "code_module", "code_presentation"], "left")
        .join(clicks,    ["id_student", "code_module", "code_presentation"], "left")
        .withColumn("num_submissions", F.coalesce(F.col("num_submissions"), F.lit(0)))
    )

    window_pres = Window.partitionBy("code_presentation")
    fact_base = fact_base.withColumn(
        "score_vs_avg",
        F.col("avg_score") - F.avg("avg_score").over(window_pres),
    )

    fact_base = fact_base.withColumn(
        "risk_group",
        F.when(F.col("avg_score") >= 70, "Low")
         .when(F.col("avg_score") >= 50, "Medium")
         .otherwise("High"),
    )

    fact = (
        fact_base
        .join(dim_student.select("id_student", "student_key"), "id_student")
        .join(dim_course.select("code_module", "code_presentation", "course_key"),
              ["code_module", "code_presentation"])
        .join(dim_time.select("code_presentation", "time_key"), "code_presentation")
        .select(
            "student_key", "course_key", "time_key",
            "avg_score", "total_clicks", "num_submissions",
            "final_result", "score_vs_avg", "risk_group",
        )
    )

    count = fact.count()
    log.info(f"[COUNT] Fact_Performance: {count:,} rows")
    fact.write.jdbc(url=JDBC_URL, table="Fact_Performance", mode="append", properties=WRITE_PROPS)
    log.info(f"[DONE ] Fact_Performance: {count:,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Spark master    : {SPARK_MASTER}")
    log.info(f"Silver bucket   : {MINIO_BUCKET_SILVER}")
    log.info(f"MySQL target    : {MYSQL_HOST}:{MYSQL_PORT}/student_dwh")

    spark = (
        SparkSession.builder
        .appName("dwh_load")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions",               "10")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        truncate_dwh_tables()

        dim_time = spark.read.jdbc(url=JDBC_URL, table="Dim_Time", properties=JDBC_PROPS).cache()
        log.info(f"[READ ] Dim_Time: {dim_time.count()} rows")

        dim_course     = build_dim_course(spark)
        dim_student    = build_dim_student(spark)
        _              = build_dim_assessment(spark)

        build_fact_performance(spark, dim_student, dim_course, dim_time)

        log.info("=== spark_silver_to_dwh COMPLETED ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

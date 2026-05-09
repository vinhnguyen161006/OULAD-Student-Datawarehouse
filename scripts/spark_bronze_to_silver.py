"""
spark_bronze_to_silver.py
==========================
DAG 02 — Đọc 7 CSV từ MinIO Bronze → validate + clean → ghi Parquet vào MinIO Silver.

Bronze (s3a://oulad-bronze/) → Silver (s3a://oulad-silver/)

Output paths:
  s3a://oulad-silver/student_info/
  s3a://oulad-silver/assessments/
  s3a://oulad-silver/vle/
  s3a://oulad-silver/student_registration/
  s3a://oulad-silver/student_assessment/
  s3a://oulad-silver/vle_clicks/    ← SUM(sum_click) GROUP BY id_student+module+presentation

Biến môi trường (inject từ Airflow):
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    MINIO_BUCKET_BRONZE, MINIO_BUCKET_SILVER
    SPARK_MASTER
"""

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "oulad-bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "oulad-silver")
SPARK_MASTER        = os.getenv("SPARK_MASTER", "local[*]")

VALID_PRESENTATIONS = {"2013J", "2013B", "2014J", "2014B"}

SCHEMA_COURSES = StructType([
    StructField("code_module",                StringType(),  True),
    StructField("code_presentation",          StringType(),  True),
    StructField("module_presentation_length", IntegerType(), True),
])

SCHEMA_ASSESSMENTS = StructType([
    StructField("code_module",       StringType(),  True),
    StructField("code_presentation", StringType(),  True),
    StructField("id_assessment",     IntegerType(), True),
    StructField("assessment_type",   StringType(),  True),
    StructField("date",              IntegerType(), True),
    StructField("weight",            FloatType(),   True),
])

SCHEMA_VLE = StructType([
    StructField("id_site",           IntegerType(), True),
    StructField("code_module",       StringType(),  True),
    StructField("code_presentation", StringType(),  True),
    StructField("activity_type",     StringType(),  True),
    StructField("week_from",         IntegerType(), True),
    StructField("week_to",           IntegerType(), True),
])

SCHEMA_STUDENT_INFO = StructType([
    StructField("code_module",           StringType(),  True),
    StructField("code_presentation",     StringType(),  True),
    StructField("id_student",            IntegerType(), True),
    StructField("gender",                StringType(),  True),
    StructField("region",                StringType(),  True),
    StructField("highest_education",     StringType(),  True),
    StructField("imd_band",              StringType(),  True),
    StructField("age_band",              StringType(),  True),
    StructField("num_of_prev_attempts",  IntegerType(), True),
    StructField("studied_credits",       IntegerType(), True),
    StructField("disability",            StringType(),  True),
    StructField("final_result",          StringType(),  True),
])

SCHEMA_STUDENT_REGISTRATION = StructType([
    StructField("code_module",         StringType(),  True),
    StructField("code_presentation",   StringType(),  True),
    StructField("id_student",          IntegerType(), True),
    StructField("date_registration",   IntegerType(), True),
    StructField("date_unregistration", IntegerType(), True),
])

SCHEMA_STUDENT_ASSESSMENT = StructType([
    StructField("id_assessment",  IntegerType(), True),
    StructField("id_student",     IntegerType(), True),
    StructField("date_submitted", IntegerType(), True),
    StructField("is_banked",      IntegerType(), True),
    StructField("score",          FloatType(),   True),
])

SCHEMA_STUDENT_VLE = StructType([
    StructField("code_module",       StringType(),  True),
    StructField("code_presentation", StringType(),  True),
    StructField("id_student",        IntegerType(), True),
    StructField("id_site",           IntegerType(), True),
    StructField("date",              IntegerType(), True),
    StructField("sum_click",         IntegerType(), True),
])


def bronze_path(filename: str) -> str:
    return f"s3a://{MINIO_BUCKET_BRONZE}/{filename}"


def silver_path(table: str) -> str:
    return f"s3a://{MINIO_BUCKET_SILVER}/{table}/"


def read_csv(spark: SparkSession, filename: str, schema: StructType):
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .csv(bronze_path(filename))
    )


def write_silver(df, table: str) -> int:
    count = df.count()
    df.write.mode("overwrite").parquet(silver_path(table))
    log.info(f"[SILVER] {table}: {count:,} rows → {silver_path(table)}")
    return count


def main():
    log.info(f"Spark master    : {SPARK_MASTER}")
    log.info(f"Bronze bucket   : {MINIO_BUCKET_BRONZE}")
    log.info(f"Silver bucket   : {MINIO_BUCKET_SILVER}")

    spark = (
        SparkSession.builder
        .appName("silver_processing")
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
        df = read_csv(spark, "courses.csv", SCHEMA_COURSES)
        df = df.filter(F.col("code_module").isNotNull())
        write_silver(df, "courses")

        df = read_csv(spark, "studentInfo.csv", SCHEMA_STUDENT_INFO)
        df = df.filter(
            F.col("id_student").isNotNull() &
            F.col("code_presentation").isin(*VALID_PRESENTATIONS)
        )
        write_silver(df, "student_info")

        df = read_csv(spark, "assessments.csv", SCHEMA_ASSESSMENTS)
        df = df.filter(
            F.col("id_assessment").isNotNull() &
            (F.col("weight").isNull() | (F.col("weight") >= 0))
        )
        write_silver(df, "assessments")

        df = read_csv(spark, "vle.csv", SCHEMA_VLE)
        df = df.filter(F.col("id_site").isNotNull())
        write_silver(df, "vle")

        df = read_csv(spark, "studentRegistration.csv", SCHEMA_STUDENT_REGISTRATION)
        df = df.filter(
            F.col("id_student").isNotNull() &
            F.col("code_presentation").isin(*VALID_PRESENTATIONS)
        )
        write_silver(df, "student_registration")

        df = read_csv(spark, "studentAssessment.csv", SCHEMA_STUDENT_ASSESSMENT)
        df = df.filter(
            F.col("id_assessment").isNotNull() &
            F.col("id_student").isNotNull() &
            (F.col("score").isNull() | ((F.col("score") >= 0) & (F.col("score") <= 100)))
        )
        write_silver(df, "student_assessment")

        # Aggregate VLE clicks: SUM(sum_click) GROUP BY (id_student, code_module, code_presentation)
        log.info("[READ ] studentVle.csv (~10M rows)")
        df_vle = read_csv(spark, "studentVle.csv", SCHEMA_STUDENT_VLE)
        df_clicks = (
            df_vle
            .filter(F.col("sum_click").isNotNull() & (F.col("sum_click") > 0))
            .groupBy("id_student", "code_module", "code_presentation")
            .agg(F.sum("sum_click").cast("long").alias("total_clicks"))
            .repartition(4)
        )
        write_silver(df_clicks, "vle_clicks")

        log.info("=== spark_bronze_to_silver COMPLETED ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

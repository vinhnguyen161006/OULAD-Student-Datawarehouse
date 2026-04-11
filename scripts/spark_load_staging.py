"""
spark_load_staging.py
=====================
DAG 1 — Đọc 7 file CSV OULAD từ data/raw/ và nạp vào 7 bảng stg_* trong MySQL student_dwh.

Chạy từ SparkSubmitOperator (client mode): driver trên Airflow container,
executors trên Spark worker.

Biến môi trường (inject từ Airflow):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
    DATA_DIR   — đường dẫn đến data/raw/ (mặc định /opt/airflow/data/raw)
    SPARK_MASTER — URL Spark master (mặc định local[*])
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Cấu hình kết nối ──────────────────────────────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT     = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpassword")
DATA_DIR       = os.getenv("DATA_DIR", "/opt/airflow/data/raw")
SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/student_dwh"
    "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
)
JDBC_PROPS = {
    "user":     MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver":   "com.mysql.cj.jdbc.Driver",
}

# ── Schema tường minh — tránh inferSchema scan toàn bộ file ──────────────────

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
    StructField("date",              IntegerType(), True),   # NULL = Exam cuối kỳ
    StructField("weight",            FloatType(),   True),
])

SCHEMA_VLE = StructType([
    StructField("id_site",           IntegerType(), True),
    StructField("code_module",       StringType(),  True),
    StructField("code_presentation", StringType(),  True),
    StructField("activity_type",     StringType(),  True),
    StructField("week_from",         IntegerType(), True),   # NULL allowed
    StructField("week_to",           IntegerType(), True),   # NULL allowed
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
    StructField("code_module",           StringType(),  True),
    StructField("code_presentation",     StringType(),  True),
    StructField("id_student",            IntegerType(), True),
    StructField("date_registration",     IntegerType(), True),
    StructField("date_unregistration",   IntegerType(), True),  # NULL nếu không rút môn
])

SCHEMA_STUDENT_ASSESSMENT = StructType([
    StructField("id_assessment",   IntegerType(), True),
    StructField("id_student",      IntegerType(), True),
    StructField("date_submitted",  IntegerType(), True),
    StructField("is_banked",       IntegerType(), True),
    StructField("score",           FloatType(),   True),   # NULL nếu chưa nộp
])

SCHEMA_STUDENT_VLE = StructType([
    StructField("code_module",       StringType(),  True),
    StructField("code_presentation", StringType(),  True),
    StructField("id_student",        IntegerType(), True),
    StructField("id_site",           IntegerType(), True),
    StructField("date",              IntegerType(), True),
    StructField("sum_click",         IntegerType(), True),
])

# ── Mapping CSV → bảng stg_* ──────────────────────────────────────────────────
# Thứ tự: nhỏ → lớn, stg_student_vle (~10M rows) cuối cùng
TABLE_MAP = [
    ("courses.csv",              "stg_courses",              SCHEMA_COURSES),
    ("assessments.csv",          "stg_assessments",          SCHEMA_ASSESSMENTS),
    ("vle.csv",                  "stg_vle",                  SCHEMA_VLE),
    ("studentInfo.csv",          "stg_student_info",         SCHEMA_STUDENT_INFO),
    ("studentRegistration.csv",  "stg_student_registration", SCHEMA_STUDENT_REGISTRATION),
    ("studentAssessment.csv",    "stg_student_assessment",   SCHEMA_STUDENT_ASSESSMENT),
    ("studentVle.csv",           "stg_student_vle",          SCHEMA_STUDENT_VLE),
]


# ── Helper: load một CSV vào một bảng stg ─────────────────────────────────────

def load_table(spark: SparkSession, csv_file: str, table: str, schema: StructType) -> None:
    path = os.path.join(DATA_DIR, csv_file)
    log.info(f"[READ ] {csv_file} → {table}")

    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")          # empty string → null
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .csv(path)
    )

    row_count = df.count()
    log.info(f"[COUNT] {table}: {row_count:,} rows")

    # stg_student_vle: repartition để write song song, giảm áp lực JDBC
    if table == "stg_student_vle":
        df = df.repartition(10)

    write_props = {**JDBC_PROPS, "batchsize": "10000", "truncate": "true"}

    (
        df.write
        .jdbc(url=JDBC_URL, table=table, mode="overwrite", properties=write_props)
    )
    log.info(f"[DONE ] {table} loaded ({row_count:,} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Spark master : {SPARK_MASTER}")
    log.info(f"Data dir     : {DATA_DIR}")
    log.info(f"MySQL target : {MYSQL_HOST}:{MYSQL_PORT}/student_dwh")

    spark = (
        SparkSession.builder
        .appName("staging_load")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for csv_file, table, schema in TABLE_MAP:
            load_table(spark, csv_file, table, schema)
        log.info("=== Staging load COMPLETED ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

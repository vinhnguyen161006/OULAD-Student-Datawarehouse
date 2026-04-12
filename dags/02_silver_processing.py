"""
DAG 02 — silver_processing
===========================
Đọc 7 CSV từ MinIO Bronze → validate + clean → ghi Parquet vào MinIO Silver.

Input  : Airflow Dataset s3://oulad-bronze
         (trigger tự động từ bronze_ingest — DAG 01)
Output : Airflow Dataset s3://oulad-silver
         (trigger tự động dwh_load — DAG 03)

Thành viên phụ trách: Tú
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BRONZE_DATASET = Dataset("s3://oulad-bronze")
SILVER_DATASET = Dataset("s3://oulad-silver")

SPARK_APP = "/opt/airflow/scripts/spark_bronze_to_silver.py"

JARS = ",".join([
    "/opt/airflow/jars/mysql-connector-j-8.0.33.jar",
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
])

_SPARK_CONF = {
    "spark.sql.shuffle.partitions":               "10",
    "spark.driver.memory":                        "2g",
    "spark.executor.memory":                      "2g",
    "spark.network.timeout":                      "600s",
    "spark.executor.heartbeatInterval":           "60s",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

_ENV_VARS = {
    "MINIO_ENDPOINT":  "http://minio:9000",
    "MINIO_ACCESS_KEY":"minioadmin",
    "MINIO_SECRET_KEY":"minioadmin",
    "MINIO_BUCKET_BRONZE": "oulad-bronze",
    "MINIO_BUCKET_SILVER": "oulad-silver",
    "SPARK_MASTER":    "spark://spark-master:7077",
}


@dag(
    dag_id="silver_processing",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "pyspark", "minio", "oulad"],
    doc_md="""
## DAG 02 — Silver Processing (Bronze → Silver Parquet)

**Mục đích**: Validate + clean dữ liệu từ Bronze CSV → ghi Parquet vào MinIO Silver.

**Input dataset**: `s3://oulad-bronze`
→ Tự động trigger bởi `bronze_ingest` (DAG 01).

**Output dataset**: `s3://oulad-silver`
→ Tự động trigger `dwh_load` (DAG 03).

**Validation rules**:
- `code_presentation` ∈ {2013J, 2013B, 2014J, 2014B}
- `score` ∈ [0, 100] hoặc NULL
- `weight` >= 0
- `id_student`, `id_assessment`, `id_site` NOT NULL

**Silver paths**:
- `s3a://oulad-silver/student_info/`
- `s3a://oulad-silver/assessments/`
- `s3a://oulad-silver/vle/`
- `s3a://oulad-silver/student_registration/`
- `s3a://oulad-silver/student_assessment/`
- `s3a://oulad-silver/vle_clicks/`  ← SUM(sum_click) aggregated
    """,
)
def silver_processing_dag():

    process = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        conn_id="spark_default",
        application=SPARK_APP,
        name="silver_processing",
        deploy_mode="client",
        jars=JARS,
        conf=_SPARK_CONF,
        env_vars=_ENV_VARS,
        verbose=True,
    )

    @task(outlets=[SILVER_DATASET])
    def publish_silver_dataset():
        """Publish dataset → trigger DAG 03 (dwh_load) tự động."""
        pass

    process >> publish_silver_dataset()


silver_processing_dag()

# DAG 02 — silver_processing: Bronze CSV → validate + clean → Silver Parquet

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

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
    default_args=DEFAULT_ARGS,
    tags=["silver", "pyspark", "minio", "oulad"],
    doc_md="""
**Input**: `s3://oulad-bronze` → trigger bởi `bronze_ingest`
**Output**: `s3://oulad-silver` → trigger `dwh_load`
**Job**: `scripts/spark_bronze_to_silver.py`
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
        pass

    process >> publish_silver_dataset()


silver_processing_dag()

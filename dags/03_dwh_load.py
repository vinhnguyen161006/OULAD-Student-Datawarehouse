# DAG 03 — dwh_load: Silver Parquet → Star Schema → MySQL student_dwh

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SILVER_DATASET    = Dataset("s3://oulad-silver")
FACT_PERF_DATASET = Dataset("mysql://student_dwh/fact_performance")

SPARK_APP = "/opt/airflow/scripts/spark_silver_to_dwh.py"

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
    "MYSQL_HOST":      "mysql",
    "MYSQL_PORT":      "3306",
    "MYSQL_USER":      "root",
    "MYSQL_PASSWORD":  "rootpassword",
    "MINIO_ENDPOINT":  "http://minio:9000",
    "MINIO_ACCESS_KEY":"minioadmin",
    "MINIO_SECRET_KEY":"minioadmin",
    "MINIO_BUCKET_SILVER": "oulad-silver",
    "SPARK_MASTER":    "spark://spark-master:7077",
}


@dag(
    dag_id="dwh_load",
    schedule=[SILVER_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dwh", "pyspark", "minio", "mysql", "oulad"],
    doc_md="""
**Input**: `s3://oulad-silver` → trigger bởi `silver_processing`
**Output**: `mysql://student_dwh/fact_performance` → trigger `gold_dbt_run`
**Job**: `scripts/spark_silver_to_dwh.py`
**Load order**: Dim_Time → Dim_Course → Dim_Student → Dim_Assessment → Fact_Performance
    """,
)
def dwh_load_dag():

    load = SparkSubmitOperator(
        task_id="spark_silver_to_dwh",
        conn_id="spark_default",
        application=SPARK_APP,
        name="dwh_load",
        deploy_mode="client",
        jars=JARS,
        conf=_SPARK_CONF,
        env_vars=_ENV_VARS,
        verbose=True,
    )

    @task(outlets=[FACT_PERF_DATASET])
    def publish_fact_dataset():
        pass

    load >> publish_fact_dataset()


dwh_load_dag()

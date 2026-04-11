"""
DAG 1 — staging_load
=====================
Đọc 7 file CSV OULAD → nạp 7 bảng stg_* trong MySQL student_dwh.

Input  : data/raw/*.csv (mount tại /opt/airflow/data/raw)
Output : Airflow Dataset mysql://student_dwh/staging
         (trigger tự động DAG 2 — silver_pyspark)

Thành viên phụ trách: Tú
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Dataset được publish sau khi staging load thành công → trigger DAG 2
STAGING_DATASET = Dataset("mysql://student_dwh/staging")

SPARK_APP      = "/opt/airflow/scripts/spark_load_staging.py"
MYSQL_JDBC_JAR = "/opt/airflow/jars/mysql-connector-j-8.0.33.jar"


@dag(
    dag_id="staging_load",
    schedule=None,           # Trigger thủ công hoặc cron tuỳ chỉnh
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["staging", "pyspark", "oulad"],
    doc_md="""
## DAG 1 — Staging Load

**Mục đích**: Nạp 7 file CSV OULAD vào lớp Staging của MySQL `student_dwh`.

**Input**: `data/raw/` — 7 file CSV gốc (không chỉnh sửa)

**Output dataset**: `mysql://student_dwh/staging`
→ Tự động trigger `silver_pyspark` (DAG 2) khi hoàn thành.

**Bảng được nạp** (theo thứ tự):
- `stg_courses` (22 rows)
- `stg_assessments` (206 rows)
- `stg_vle` (6.364 rows)
- `stg_student_info` (~32.593 rows)
- `stg_student_registration` (~32.593 rows)
- `stg_student_assessment` (~173.912 rows)
- `stg_student_vle` (~10.655.280 rows)

**Chú ý**: Mỗi lần chạy sẽ TRUNCATE + INSERT lại (mode=overwrite) — idempotent.
    """,
)
def staging_load_dag():

    spark_load = SparkSubmitOperator(
        task_id="spark_load_staging",
        conn_id="spark_default",
        application=SPARK_APP,
        name="staging_load",
        deploy_mode="client",
        jars=MYSQL_JDBC_JAR,
        conf={
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s",
        },
        env_vars={
            "MYSQL_HOST":     "mysql",
            "MYSQL_PORT":     "3306",
            "MYSQL_USER":     "root",
            "MYSQL_PASSWORD": "rootpassword",
            "DATA_DIR":       "/opt/airflow/data/raw",
            "SPARK_MASTER":   "spark://spark-master:7077",
        },
        verbose=True,
    )

    # Publish Dataset sau khi Spark job thành công → trigger DAG 2
    @task(outlets=[STAGING_DATASET])
    def publish_staging_dataset():
        pass

    spark_load >> publish_staging_dataset()


staging_load_dag()

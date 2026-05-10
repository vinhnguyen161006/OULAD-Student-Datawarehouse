# DAG 04 — gold_dbt_run: student_dwh → dbt build → student_data_mart

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

FACT_PERF_DATASET = Dataset("mysql://student_dwh/fact_performance")
MART_DATASET      = Dataset("mysql://student_data_mart")

DBT_PROJECT_DIR = "/opt/dbt_student"
DBT_PROFILES_DIR = "/opt/dbt_student"

_DBT_ENV = {
    "DBT_HOST":      "mysql",
    "DBT_USER":      "root",
    "DBT_PASSWORD":  "rootpassword",
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
}

_DBT_FLAGS = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR}"
)


@dag(
    dag_id="gold_dbt_run",
    schedule=[FACT_PERF_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "dbt", "mysql", "oulad"],
    doc_md="""
**Input**: `mysql://student_dwh/fact_performance` → trigger bởi `dwh_load`
**Output**: `mysql://student_data_mart` → trigger `monitoring`

**Flow**:
1. `dbt_deps` — cài dbt packages (idempotent)
2. `dbt_run`  — build staging views + mart tables vào `student_data_mart`
3. `dbt_test` — chạy data tests trong `schema.yml`
4. `publish_mart_dataset` — phát dataset cho DAG monitoring

Project path: `/opt/dbt_student` (mount vào container scheduler).
    """,
)
def gold_dbt_run_dag():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps {_DBT_FLAGS}",
        env=_DBT_ENV,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {_DBT_FLAGS}",
        env=_DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {_DBT_FLAGS}",
        env=_DBT_ENV,
        append_env=True,
    )

    @task(outlets=[MART_DATASET])
    def publish_mart_dataset():
        pass

    dbt_deps >> dbt_run >> dbt_test >> publish_mart_dataset()


gold_dbt_run_dag()

# CLAUDE.md — Student Data Warehouse Project

## Tổng quan dự án

Kho dữ liệu phân tích kết quả học tập sinh viên Đại học Mở (OULAD dataset) theo kiến trúc **ETL 4 lớp (Bronze → Silver → DWH → Mart)**. Orchestration bằng Apache Airflow, transform bằng PySpark, object storage bằng MinIO, warehouse trên MySQL, mart modeling bằng dbt, dashboard bằng Metabase — toàn bộ chạy local qua Docker Compose.

## Tech stack

| Lớp | Công nghệ | Vai trò |
|-----|-----------|---------|
| Orchestration | Apache Airflow | Điều phối pipeline, event-driven qua Airflow Datasets |
| Processing | PySpark | ETL — đọc CSV, validate, transform, load |
| Object Storage | MinIO | Bronze (CSV) + Silver (Parquet) data lake |
| Warehouse | MySQL 8.0 | DWH (`student_dwh`) + Marts (`student_data_mart`) |
| Mart Modeling | dbt | Build Gold models, chạy data tests |
| BI | Metabase | Dashboard kết nối vào mart |
| Runtime | Docker Compose | Chạy toàn bộ stack local |
| Source control | Git + GitHub | Quản lý mã nguồn |

## Kiến trúc ETL

```
[7 file CSV OULAD]
        │ make upload-data
        ▼
  ┌─────────────┐
  │   BRONZE    │  MinIO: oulad-bronze (CSV gốc)
  └──────┬──────┘
         │ DAG 02 — spark_bronze_to_silver.py
         ▼
  ┌─────────────┐
  │   SILVER    │  MinIO: oulad-silver (Parquet, validated)
  └──────┬──────┘
         │ DAG 03 — spark_silver_to_dwh.py
         ▼
  ┌─────────────┐
  │     DWH     │  MySQL: student_dwh
  │ (Star Schema)│  Dim_* + Fact_Performance
  └──────┬──────┘
         │ DAG 04 — dbt build
         ▼
  ┌─────────────┐
  │    MARTS    │  MySQL: student_data_mart
  │  (dbt Gold) │  mart_* aggregations + tests
  └──────┬──────┘
         │ DAG 05 — monitoring
         ▼
   Health checks + monitoring_log
         │
         ▼
   Metabase Dashboards
```

## Cấu trúc thư mục

```
Student-Data-Warehouse/
├── .github/
├── dags/
│   ├── 01_bronze_ingest.py      # Kiểm tra CSV đủ trong MinIO Bronze → publish dataset
│   ├── 02_silver_processing.py  # Bronze CSV → Silver Parquet (validate + clean)
│   ├── 03_dwh_load.py           # Silver Parquet → MySQL Dim_* + Fact_Performance
│   ├── 04_gold_dbt_run.py       # dbt build + test → student_data_mart
│   └── 05_monitoring.py         # Health checks + monitoring_log
├── data/
│   └── raw/                     # 7 file CSV OULAD gốc (gitignored, tải bằng make setup-data)
├── dbt_student/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/
│   ├── airflow/
│   ├── spark/
│   └── mysql/                   # init.sql — DWH schema + monitoring_log
├── docs/
├── scripts/
│   ├── upload_to_minio.py       # Upload CSV → MinIO Bronze
│   ├── spark_bronze_to_silver.py# Bronze CSV → Silver Parquet
│   ├── spark_silver_to_dwh.py   # Silver Parquet → MySQL DWH
│   └── spark_monitoring.py      # Quality checks
├── download_data.ps1
├── .env
├── .gitignore
├── docker-compose.yml
├── Makefile
└── README.md
```

## Pipeline — 5 DAGs (event-driven)

```
01_bronze_ingest
  ──s3://oulad-bronze──▶ 02_silver_processing
    ──s3://oulad-silver──▶ 03_dwh_load
      ──mysql://student_dwh/fact_performance──▶ 04_gold_dbt_run
        ──mysql://student_data_mart/──▶ 05_monitoring
```

### DAG 01 — `01_bronze_ingest.py`
- Kiểm tra 7 CSV files có đủ trong MinIO `oulad-bronze` chưa
- Publish Dataset: `s3://oulad-bronze`

### DAG 02 — `02_silver_processing.py`
- Trigger khi `s3://oulad-bronze` available
- PySpark đọc Bronze CSV → validate → ghi Silver Parquet vào `oulad-silver/`
- Output: `student_info/`, `assessments/`, `vle/`, `student_registration/`, `student_assessment/`, `vle_clicks/`
- Publish Dataset: `s3://oulad-silver`

### DAG 03 — `03_dwh_load.py`
- Trigger khi `s3://oulad-silver` available
- PySpark đọc Silver Parquet → build Dim_* + Fact_Performance → load MySQL
- Publish Dataset: `mysql://student_dwh/fact_performance`

### DAG 04 — `04_gold_dbt_run.py`
- Trigger khi `mysql://student_dwh/fact_performance` available
- Chạy `dbt build` → student_data_mart
- Publish Dataset: `mysql://student_data_mart/`

### DAG 05 — `05_monitoring.py`
- Trigger khi `mysql://student_data_mart/` available
- Kiểm tra row counts, NULL rates, 4 kỳ học
- Ghi kết quả vào `monitoring_log`

## Mô hình dữ liệu

### Silver layer — MinIO oulad-silver (Parquet)

| Path | Nguồn | Mô tả |
|------|-------|-------|
| `student_info/` | studentInfo.csv | Validated, code_presentation checked |
| `assessments/` | assessments.csv | weight >= 0 |
| `vle/` | vle.csv | id_site NOT NULL |
| `student_registration/` | studentRegistration.csv | — |
| `student_assessment/` | studentAssessment.csv | score 0-100 hoặc NULL |
| `vle_clicks/` | studentVle.csv | SUM(sum_click) GROUP BY id_student+module+presentation |

### DWH — Star Schema trong `student_dwh`

**Dim_Student** — id_student, gender, region, highest_education, imd_band, age_band, disability, num_of_prev_attempts, studied_credits

**Dim_Course** — code_module, code_presentation, module_presentation_length

**Dim_Time** — code_presentation, year, semester_type, presentation_label *(4 bản ghi cố định)*

**Dim_Assessment** — id_assessment, code_module, assessment_type, weight, day_due

**Fact_Performance** *(grain: sinh viên × course presentation)*
- avg_score, total_clicks, num_submissions, final_result, score_vs_avg, risk_group

### student_data_mart — dbt models (Gold)

| Model | Mô tả |
|-------|-------|
| `mart_result_by_module` | Phân bố kết quả theo module + kỳ học |
| `mart_score_by_gender` | Điểm trung bình theo giới tính |
| `mart_education_impact` | Tác động trình độ học vấn phụ huynh |
| `mart_vle_engagement` | Tương quan click rate vs avg_score |
| `mart_at_risk_students` | Danh sách sinh viên High Risk |

## Kết nối dịch vụ (Docker Compose defaults)

| Dịch vụ | Host:Port | Credential |
|---------|-----------|------------|
| Airflow UI | localhost:8080 | admin / admin |
| MySQL | localhost:3306 | root / rootpassword |
| MinIO Console | localhost:9001 | minioadmin / minioadmin |
| MinIO API | localhost:9000 | minioadmin / minioadmin |
| Metabase | localhost:3000 | — (setup lần đầu) |
| Spark Master UI | localhost:8081 | — |

## Phân công

| Thành viên | Phụ trách | File chính |
|-----------|-----------|------------|
| Tùng | Docker Compose, infrastructure, schema init | `docker-compose.yml`, `docker/mysql/init.sql`, `Makefile` |
| Tú | DAG 01 Bronze + DAG 02 Silver processing | `dags/01_bronze_ingest.py`, `dags/02_silver_processing.py`, `scripts/spark_bronze_to_silver.py` |
| Vinh | DAG 03 DWH load | `dags/03_dwh_load.py`, `scripts/spark_silver_to_dwh.py` |
| Quang & Đức | DAG 04 dbt Marts | `dags/04_gold_dbt_run.py`, `dbt_student/models/` |
| Long | DAG 05 Monitoring + Metabase + docs | `dags/05_monitoring.py`, `scripts/spark_monitoring.py`, `docs/` |

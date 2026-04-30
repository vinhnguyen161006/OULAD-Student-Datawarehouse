# OULAD Student Data Warehouse Project

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
                           CSV OULAD
                              │
                              ▼
                        ┌─────────────┐
                        │ 01 | BRONZE │
                        │   Ingest    │
                        └──────┬──────┘
                        s3://oulad-bronze
                              ▼
                        ┌─────────────┐
                        │ 02 | SILVER │
                        │  Processing │
                        └──────┬──────┘
                        s3://oulad-silver
                              ▼
                        ┌─────────────┐
                        │ 03 |  DWH   │
                        │    Load     │
                        └──────┬──────┘
                        mysql://student_dwh
                              ▼
                        ┌─────────────┐
                        │ 04 | GOLD   │
                        │    (dbt)    │
                        └──────┬──────┘
                        mysql://student_data_mart
                              ▼
                        ┌─────────────┐
                        │ 05 | MONITOR│
                        │   Health    │
                        └─────────────┘
                              ▼
                           Metabase
```

### Bảng DAGs

| # | DAG | Trigger | Task | Output |
|:-:|-----|---------|------|--------|
| **01** | Bronze Ingest | Manual/Schedule | Kiểm tra 7 CSV files | `s3://oulad-bronze` |
| **02** | Silver Processing | s3://oulad-bronze | PySpark: validate + clean → Parquet | `s3://oulad-silver` |
| **03** | DWH Load | s3://oulad-silver | PySpark: build Dim_* + Fact_Performance | `mysql://student_dwh` |
| **04** | Gold (dbt) | mysql://student_dwh | dbt: build + test marts | `mysql://student_data_mart` |
| **05** | Monitoring | mysql://student_data_mart | Check row counts, NULL rates, metrics | `monitoring_log` |

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

| Bảng | Cột chính |
|------|----------|
| **Dim_Student** | id_student, gender, region, highest_education, imd_band, age_band, disability, num_of_prev_attempts, studied_credits |
| **Dim_Course** | code_module, code_presentation, module_presentation_length |
| **Dim_Time** | code_presentation, year, semester_type, presentation_label *(4 bản ghi cố định)* |
| **Dim_Assessment** | id_assessment, code_module, assessment_type, weight, day_due |
| **Fact_Performance** | avg_score, total_clicks, num_submissions, final_result, score_vs_avg, risk_group |

*Fact_Performance grain: sinh viên × course presentation*

### student_data_mart — dbt models (Gold)

| Model | Mô tả |
|-------|-------|
| `mart_result_by_module` | Phân bố kết quả theo module + kỳ học |
| `mart_score_by_gender` | Điểm trung bình theo giới tính |
| `mart_education_impact` | Tác động trình độ học vấn phụ huynh |
| `mart_vle_engagement` | Tương quan click rate vs avg_score |
| `mart_at_risk_students` | Danh sách sinh viên High Risk |

## 🌐 Kết nối dịch vụ (Docker Compose defaults)

| Dịch vụ | Host:Port | Credential |
|---------|-----------|------------|
| Airflow UI | localhost:8080 | admin / admin |
| MySQL | localhost:3307 | root / rootpassword |
| MinIO Console | localhost:9001 | minioadmin / minioadmin |
| MinIO API | localhost:9000 | minioadmin / minioadmin |
| Metabase | localhost:3000 | — (setup lần đầu) |
| Spark Master UI | localhost:8081 | — |

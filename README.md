# CLAUDE.md — Student Data Warehouse Project

## Tổng quan dự án

Kho dữ liệu phân tích kết quả học tập sinh viên Đại học Mở (OULAD dataset) theo kiến trúc **ETL 3 lớp (Staging → DWH → Mart)**. Orchestration bằng Apache Airflow, transform bằng PySpark, warehouse trên MySQL, mart modeling bằng dbt, dashboard bằng Metabase — toàn bộ chạy local qua Docker Compose.

## Tech stack

| Lớp | Công nghệ | Vai trò |
|-----|-----------|---------|
| Orchestration | Apache Airflow | Điều phối pipeline, event-driven qua Airflow Datasets |
| Processing | PySpark | ETL — đọc CSV, transform, load staging + DWH |
| Warehouse | MySQL 8.0 | Staging + DWH (`student_dwh`) + Marts (`student_data_mart`) |
| Mart Modeling | dbt | Build Gold models, chạy data tests |
| BI | Metabase | Dashboard kết nối vào mart |
| Runtime | Docker Compose | Chạy toàn bộ stack local |
| Source control | Git + GitHub | Quản lý mã nguồn |

## Kiến trúc ETL

```
[7 file CSV OULAD]
        │
        ▼ DAG 1 — PySpark load
  ┌─────────────┐
  │   STAGING   │  MySQL: student_dwh (bảng stg_*)
  │  (raw data) │  Giữ nguyên dữ liệu gốc
  └──────┬──────┘
         │ DAG 2 — PySpark transform
         ▼
  ┌─────────────┐
  │     DWH     │  MySQL: student_dwh
  │ (Star Schema)│  Dim_* + Fact_Performance
  └──────┬──────┘
         │ DAG 3 — dbt build
         ▼
  ┌─────────────┐
  │    MARTS    │  MySQL: student_data_mart
  │  (dbt Gold) │  mart_* aggregations + tests
  └──────┬──────┘
         │
         ▼
   Metabase Dashboards
```

## Cấu trúc thư mục

```
Student-Data-Warehouse/
├── .github/                   # (Tùy chọn) GitHub Actions CI/CD
├── dags/                      # Airflow DAGs
│   ├── 01_staging_load.py     # Đọc CSV → nạp bảng stg_* trong MySQL
│   ├── 02_silver_pyspark.py   # PySpark transform → Star Schema (student_dwh)
│   └── 03_gold_dbt_run.py     # dbt build + test → student_data_mart
├── data/
│   └── raw/                   # 7 file CSV OULAD gốc, KHÔNG chỉnh sửa
├── dbt_student/               # dbt project
│   ├── models/
│   │   ├── staging/           # View trung gian làm sạch dữ liệu
│   │   └── marts/             # mart_* bảng phân tích
│   ├── tests/                 # dbt data tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/
│   ├── airflow/               # Dockerfile Airflow (cài thêm thư viện)
│   ├── spark/                 # Dockerfile PySpark
│   └── mysql/                 # init.sql — tự động tạo schema khi start
├── docs/
│   ├── architecture.png       # Sơ đồ luồng dữ liệu
│   └── proposal.md            # Đề xuất dự án
├── scripts/                   # PySpark jobs, tách biệt khỏi Airflow
│   ├── spark_load_staging.py  # Đọc 7 CSV → nạp 7 bảng stg_*
│   ├── spark_clean_vle.py     # Xử lý stg_student_vle (~10M rows)
│   └── spark_build_fact.py    # Tính avg_score, total_clicks, risk_group → Fact
├── .env                       # Biến môi trường (MySQL pwd) — không commit
├── .gitignore
├── docker-compose.yml
├── Makefile                   # Lệnh tắt: make up, make down, make dbt
├── requirements.txt           # apache-airflow, pyspark, dbt-mysql, ...
└── README.md
```

## Pipeline — 3 DAGs (event-driven)

```
01_staging_load ──dataset──▶ 02_silver_pyspark ──dataset──▶ 03_gold_dbt_run
```

Cross-DAG dependency dùng **Airflow Datasets** — DAG sau tự động trigger khi DAG trước hoàn thành.

### DAG 1 — `01_staging_load.py`
- Gọi `scripts/spark_load_staging.py`
- Đọc 7 file CSV từ `data/raw/` bằng PySpark
- Nạp vào 7 bảng `stg_*` trong MySQL `student_dwh`, giữ nguyên dữ liệu gốc
- Publish Airflow Dataset: `mysql://student_dwh/staging`

### DAG 2 — `02_silver_pyspark.py`
- Trigger khi `mysql://student_dwh/staging` available
- Gọi PySpark jobs trong `scripts/`:
  - `spark_clean_vle.py` — xử lý `stg_student_vle` (~10M rows), aggregate total_clicks
  - `spark_build_fact.py` — tính `avg_score`, `score_vs_avg`, `risk_group`, load `Dim_*` + `Fact_Performance`
- Publish Dataset: `mysql://student_dwh/fact_performance`

### DAG 3 — `03_gold_dbt_run.py`
- Trigger khi `mysql://student_dwh/fact_performance` available
- Chạy `dbt build --project-dir dbt_student`
- Build staging + mart models trong `student_data_mart`
- Chạy dbt tests — fail DAG nếu test fail
- Publish Dataset: `mysql://student_data_mart/`

## Mô hình dữ liệu

### Staging — bảng `stg_*` trong `student_dwh`

```
stg_student_info            ← studentInfo.csv       (~32.593 rows)
stg_courses                 ← courses.csv            (22 rows)
stg_assessments             ← assessments.csv        (206 rows)
stg_student_assessment      ← studentAssessment.csv  (~173.912 rows)
stg_student_registration    ← studentRegistration.csv (~32.593 rows)
stg_student_vle             ← studentVle.csv         (~10.655.280 rows)
stg_vle                     ← vle.csv                (6.364 rows)
```

### student_dwh — Star Schema

**Dim_Student**
```sql
student_key INT PK, id_student INT UNIQUE,
gender, region, highest_education, imd_band, age_band, disability,
num_of_prev_attempts INT, studied_credits INT
```

**Dim_Course**
```sql
course_key INT PK, code_module VARCHAR(10), code_presentation VARCHAR(10),
module_presentation_length INT
```

**Dim_Time** *(4 bản ghi thật: `2013J`, `2013B`, `2014J`, `2014B`)*
```sql
time_key INT PK, code_presentation VARCHAR(10),
year INT, semester_type VARCHAR(5),   -- J / B
presentation_label VARCHAR(20)
```

**Dim_Assessment**
```sql
assessment_key INT PK, id_assessment INT UNIQUE,
code_module, assessment_type VARCHAR(10),   -- TMA / CMA / Exam
weight FLOAT, day_due INT
```

**Fact_Performance** *(grain: sinh viên × course presentation)*
```sql
fact_id INT PK,
student_key FK, course_key FK, time_key FK,
avg_score FLOAT,        -- weighted average score
total_clicks BIGINT,    -- tổng VLE interactions
num_submissions INT,
final_result VARCHAR(15),   -- Pass / Fail / Withdrawn / Distinction
score_vs_avg FLOAT,
risk_group VARCHAR(10)      -- Low / Medium / High
```

### student_data_mart — dbt models (Gold)
x
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
| Metabase | localhost:3000 | — (setup lần đầu) |
| Spark Master UI | localhost:8081 | — |

## Phân công

| Thành viên | Phụ trách | File chính |
|-----------|-----------|------------|
| Tùng | Docker Compose, infrastructure, schema init | `docker-compose.yml`, `docker/mysql/init.sql`, `Makefile` |
| Tú | DAG 1 — Staging load | `dags/01_staging_load.py`, `scripts/spark_load_staging.py` |
| Vinh | DAG 2 — PySpark transform → DWH | `dags/02_silver_pyspark.py`, `scripts/spark_clean_vle.py`, `scripts/spark_build_fact.py` |
| Quang & Đức | DAG 3 — dbt Marts | `dags/03_gold_dbt_run.py`, `dbt_student/models/` |
| Long | Metabase dashboards + docs | `docs/`, `README.md` |
# OULAD-Student-Datawarehouse

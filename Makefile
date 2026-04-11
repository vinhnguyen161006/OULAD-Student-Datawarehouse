.PHONY: up down restart build logs airflow-logs mysql-shell dbt-run dbt-test clean help

## up: Khởi động toàn bộ stack (detached)
up:
	docker compose --env-file .env up -d

## down: Dừng stack, giữ nguyên volume
down:
	docker compose down

## restart: Restart tất cả services
restart:
	docker compose restart

## build: Build lại các custom image (airflow, spark)
build:
	docker compose --env-file .env build --no-cache

## logs: Xem log tất cả services (follow)
logs:
	docker compose logs -f

## airflow-logs: Xem log Airflow webserver + scheduler
airflow-logs:
	docker compose logs -f airflow-webserver airflow-scheduler

## mysql-shell: Mở MySQL shell
mysql-shell:
	docker compose exec mysql mysql -u root -prootpassword

## dbt-run: Chạy dbt build bên trong container scheduler
dbt-run:
	docker compose exec airflow-scheduler \
		dbt build --project-dir /opt/dbt_student --profiles-dir /opt/dbt_student

## dbt-test: Chỉ chạy dbt test
dbt-test:
	docker compose exec airflow-scheduler \
		dbt test --project-dir /opt/dbt_student --profiles-dir /opt/dbt_student

## clean: Dừng stack VÀ xoá toàn bộ volume (cẩn thận!)
clean:
	docker compose down -v

## help: Hiển thị danh sách lệnh
help:
	@grep -E '^## ' Makefile | sed 's/## //'

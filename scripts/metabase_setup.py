"""
metabase_setup.py
=================
Idempotent Metabase bootstrap — chạy được trên Metabase fresh hoặc đã setup.

1. Đợi Metabase healthy
2. Setup admin (lần đầu) hoặc login (lần sau)
3. Tạo / verify 2 MySQL databases (DWH + Mart) — sync schema
4. Import / replace 4 dashboards từ `metabase/dashboards/*.json`

Mỗi dashboard JSON tham chiếu database bằng *tên*, không phải ID — nhờ vậy chạy
được trên Metabase instance mới (ID khác).

Khi dashboard cùng name đã tồn tại: cards cũ bị xoá, cards mới được tạo lại.
Idempotent — chạy nhiều lần ra cùng kết quả.

Chạy:
    python scripts/metabase_setup.py

ENV:
    METABASE_URL              (default: http://localhost:3000)
    METABASE_ADMIN_EMAIL      (default: admin@oulad.local)
    METABASE_ADMIN_PASSWORD   (default: oulad12345)
    METABASE_SITE_NAME        (default: OULAD Student Datawarehouse)
    MYSQL_HOST_INSIDE_NETWORK (default: mysql)  -- hostname từ Metabase container nhìn MySQL
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Force UTF-8 stdout (Windows fix)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@oulad.local")
ADMIN_PASS = os.getenv("METABASE_ADMIN_PASSWORD", "oulad12345")
SITE_NAME = os.getenv("METABASE_SITE_NAME", "OULAD Student Datawarehouse")

MYSQL_HOST = os.getenv("MYSQL_HOST_INSIDE_NETWORK", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpassword")

DATABASES = [
    {
        "name": "OULAD Student DWH",
        "engine": "mysql",
        "details": {
            "host": MYSQL_HOST,
            "port": 3306,
            "dbname": "student_dwh",
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
        },
    },
    {
        "name": "OULAD Student Data Mart",
        "engine": "mysql",
        "details": {
            "host": MYSQL_HOST,
            "port": 3306,
            "dbname": "student_data_mart",
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
        },
    },
]


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def req(method: str, path: str, body=None, session: str | None = None, timeout: int = 60):
    url = METABASE_URL + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if session:
        request.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            text = resp.read()
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        sys.stderr.write(f"HTTP {e.code} on {method} {path}: {body}\n")
        raise


def wait_for_metabase(timeout_seconds: int = 180) -> None:
    print(f"[wait] Metabase at {METABASE_URL} ...", end="", flush=True)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            req("GET", "/api/health", timeout=5)
            print(" ready")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(2)
    raise RuntimeError("Metabase did not become healthy in time")


# ---------------------------------------------------------------------------
# Setup / login
# ---------------------------------------------------------------------------

def setup_or_login() -> str:
    """Trả về session id. Chạy setup lần đầu, hoặc login nếu đã setup."""
    props = req("GET", "/api/session/properties")
    # `has-user-setup=False` mới là tín hiệu chưa có admin nào.
    # `setup-token` vẫn còn ngay cả sau khi setup xong → không dùng để check.
    if not props.get("has-user-setup", True) and props.get("setup-token"):
        print("[setup] First-time setup, creating admin user")
        result = req("POST", "/api/setup", {
            "token": props["setup-token"],
            "prefs": {
                "site_name": SITE_NAME,
                "site_locale": "en",
                "allow_tracking": False,
            },
            "user": {
                "first_name": "Admin",
                "last_name": "OULAD",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASS,
                "password_confirm": ADMIN_PASS,
                "site_name": SITE_NAME,
            },
        })
        return result["id"]

    print(f"[login] Existing instance, login as {ADMIN_EMAIL}")
    return req("POST", "/api/session", {
        "username": ADMIN_EMAIL,
        "password": ADMIN_PASS,
    })["id"]


# ---------------------------------------------------------------------------
# Databases
# ---------------------------------------------------------------------------

def upsert_database(spec: dict, session: str) -> int:
    name = spec["name"]
    existing = next(
        (db for db in req("GET", "/api/database", session=session).get("data", [])
         if db["name"] == name),
        None,
    )
    if existing:
        print(f"  [skip] '{name}' (id={existing['id']}) đã tồn tại")
        db_id = existing["id"]
    else:
        body = {**spec, "is_full_sync": True, "is_on_demand": False}
        created = req("POST", "/api/database", body, session=session)
        db_id = created["id"]
        print(f"  [add ] '{name}' (id={db_id})")

    # Trigger sync (idempotent — không tốn nhiều)
    try:
        req("POST", f"/api/database/{db_id}/sync_schema", session=session)
    except Exception as e:
        print(f"  [warn] sync_schema lỗi cho {name}: {e}")
    return db_id


# ---------------------------------------------------------------------------
# Dashboards
# ---------------------------------------------------------------------------

def find_dashboard_by_name(name: str, session: str):
    for d in req("GET", "/api/dashboard", session=session):
        if d["name"] == name:
            return d
    return None


def upsert_dashboard(dash: dict, name_to_db_id: dict, session: str) -> None:
    name = dash["name"]
    description = dash.get("description") or ""
    existing = find_dashboard_by_name(name, session)

    if existing:
        d_id = existing["id"]
        # Update description nếu đổi
        if existing.get("description") != description:
            req("PUT", f"/api/dashboard/{d_id}",
                {"description": description}, session=session)
        # Archive các card cũ thuộc dashboard này — tránh leak orphan khi re-import
        try:
            current = req("GET", f"/api/dashboard/{d_id}", session=session)
            old_card_ids = [dc.get("card_id") for dc in current.get("dashcards", []) if dc.get("card_id")]
            for cid in old_card_ids:
                try:
                    req("PUT", f"/api/card/{cid}", {"archived": True}, session=session)
                except Exception:
                    pass
            if old_card_ids:
                print(f"  [upd ] '{name}' (id={d_id}) — archive {len(old_card_ids)} old cards + replace")
            else:
                print(f"  [upd ] '{name}' (id={d_id}) — replace cards")
        except Exception:
            print(f"  [upd ] '{name}' (id={d_id}) — replace cards")
    else:
        created = req("POST", "/api/dashboard", {
            "name": name,
            "description": description,
        }, session=session)
        d_id = created["id"]
        print(f"  [add ] '{name}' (id={d_id})")

    # Tạo từng card và build dashcards array
    dashcards = []
    for i, c in enumerate(dash["cards"]):
        pos = c["position"]
        card_type = c.get("type", "card")

        if card_type == "text":
            # Markdown text card — không có card_id thực, chỉ là virtual_card
            display = c.get("display", "text")  # "text" hoặc "heading"
            dashcards.append({
                "id": -(i + 1),
                "card_id": None,
                "row": pos["row"],
                "col": pos["col"],
                "size_x": pos["size_x"],
                "size_y": pos["size_y"],
                "visualization_settings": {
                    "virtual_card": {
                        "name": None,
                        "display": display,
                        "visualization_settings": {},
                        "dataset_query": {},
                        "archived": False,
                    },
                    "text": c["text"],
                },
            })
            continue

        # Card thường (native SQL)
        db_name = c["database"]
        if db_name not in name_to_db_id:
            print(f"     [SKIP card '{c['name']}'] database '{db_name}' chưa được setup")
            continue

        card = req("POST", "/api/card", {
            "name": c["name"],
            "display": c["display"],
            "visualization_settings": c.get("visualization_settings", {}),
            "dataset_query": {
                "type": "native",
                "database": name_to_db_id[db_name],
                "native": {"query": c["query"]},
            },
        }, session=session)

        dashcards.append({
            "id": -(i + 1),
            "card_id": card["id"],
            "row": pos["row"],
            "col": pos["col"],
            "size_x": pos["size_x"],
            "size_y": pos["size_y"],
        })

    req("PUT", f"/api/dashboard/{d_id}", {"dashcards": dashcards}, session=session)
    n_text = sum(1 for c in dash["cards"] if c.get("type") == "text")
    n_chart = len(dashcards) - n_text
    print(f"     {n_chart} chart cards + {n_text} text cards")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_metabase()
    session = setup_or_login()

    print("\n[step] Upsert databases")
    name_to_db_id = {}
    for spec in DATABASES:
        name_to_db_id[spec["name"]] = upsert_database(spec, session)

    # Đợi sync metadata để cards reference table không lỗi
    print("\n[wait] Database sync (5s)...")
    time.sleep(5)

    print("\n[step] Upsert dashboards")
    dash_dir = Path(__file__).resolve().parent.parent / "metabase" / "dashboards"
    json_files = sorted(dash_dir.glob("*.json"))
    if not json_files:
        sys.stderr.write(f"Không tìm thấy dashboard JSON trong {dash_dir}\n")
        sys.exit(1)

    for json_file in json_files:
        with open(json_file, encoding="utf-8") as f:
            dash = json.load(f)
        upsert_dashboard(dash, name_to_db_id, session)

    print(f"\nDone. Mở {METABASE_URL} để xem dashboards.")


if __name__ == "__main__":
    main()

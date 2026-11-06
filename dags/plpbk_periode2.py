import json
import logging
import os
from datetime import datetime, timedelta

import pymysql
import requests
from dotenv import load_dotenv

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

load_dotenv("/opt/airflow/repo/.env")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}

DDL = """
    CREATE TABLE IF NOT EXISTS plpbk_periode (
        id INT PRIMARY KEY,
        periode_id INT NULL,
        schedule_name VARCHAR(255) NULL,
        schedule_description TEXT NULL,
        class_name VARCHAR(255) NULL,
        schedule_execution VARCHAR(255) NULL,
        schedule_price VARCHAR(255) NULL,
        psikolog_name VARCHAR(255) NULL,
        raw_response JSON NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4
"""

# Untuk tabel yang sudah terbuat sebelum kolom psikolog_name ditambahkan (sintaks MariaDB).
MIGRATE = """
    ALTER TABLE plpbk_periode
        ADD COLUMN IF NOT EXISTS psikolog_name VARCHAR(255) NULL AFTER schedule_price
"""

UPSERT = """
    INSERT INTO plpbk_periode (
        id, periode_id, schedule_name, schedule_description,
        class_name, schedule_execution, schedule_price, psikolog_name, raw_response
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        periode_id = VALUES(periode_id),
        schedule_name = VALUES(schedule_name),
        schedule_description = VALUES(schedule_description),
        class_name = VALUES(class_name),
        schedule_execution = VALUES(schedule_execution),
        schedule_price = VALUES(schedule_price),
        psikolog_name = VALUES(psikolog_name),
        raw_response = VALUES(raw_response)
"""


def _require_env(*keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise ValueError("Konfigurasi .env belum lengkap: " + ", ".join(missing))


def _to_int(value):
    return int(value) if value not in (None, "") else None


@dag(
    dag_id="plpbk_periode",
    schedule="1 0 * * *",
    start_date=datetime(2026, 11, 6),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["plpbk", "etl", "mariadb"],
)
def plpbk_periode():

    @task
    def tarik_dan_simpan_periode():
        # ---------------- LOGIN ----------------
        _require_env("PLPBK_BASE_URL", "PLPBK_USERNAME", "PLPBK_PASSWORD")
        base_url = os.getenv("PLPBK_BASE_URL").rstrip("/")

        resp = requests.post(
            f"{base_url}/login",
            json={
                "username": os.getenv("PLPBK_USERNAME"),
                "password": os.getenv("PLPBK_PASSWORD"),
            },
            headers=HEADERS,
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(f"Login PLPBK gagal: HTTP {resp.status_code} | {resp.text[:300]}")

        body = resp.json()
        token = None
        if isinstance(body, dict):
            d = body.get("data") if isinstance(body.get("data"), dict) else {}
            token = body.get("token") or body.get("access_token") or d.get("token") or d.get("access_token")
        if not token:
            raise ValueError("Login berhasil tetapi token tidak ditemukan dalam respons API.")

        # ---------------- EXTRACT ----------------
        resp = requests.get(
            f"{base_url}/periode-active",
            headers={**HEADERS, "Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if not resp.ok:
            raise RuntimeError(f"GET /periode-active gagal: HTTP {resp.status_code} | {resp.text[:300]}")

        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data")
        if not isinstance(data, list):
            raise ValueError("Format respons tidak sesuai. Diharapkan JSON array (atau {'data': [...]}).")

        # Respons: list periode aktif, tiap periode berisi list jadwal di "schedule_periode".
        # Diratakan menjadi satu baris per jadwal.
        schedules = []
        for periode in data:
            items = periode.get("schedule_periode") if isinstance(periode, dict) else None
            for item in items or []:
                if isinstance(item, dict):
                    schedules.append((periode, item))

        log.info(
            "Berhasil mengambil %d periode aktif (%d jadwal) dari PLPBK.",
            len(data), len(schedules),
        )
        if not schedules:
            return

        # ---------------- TRANSFORM & LOAD ----------------
        _require_env("DB_NAME", "DB_USER", "DB_PASSWORD")
        db = None
        processed = skipped = 0
        try:
            # Password tidak ditaruh di variabel lokal agar tidak tercetak di traceback.
            try:
                db = pymysql.connect(
                    host=os.getenv("DB_HOST", "localhost"),
                    port=int(os.getenv("DB_PORT", "3306")),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    database=os.getenv("DB_NAME"),
                    charset="utf8mb4",
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=False,
                )
            except pymysql.MySQLError as e:
                raise RuntimeError(f"Gagal konek MariaDB: {type(e).__name__} {e.args}") from None

            with db.cursor() as cursor:
                cursor.execute(DDL)
                cursor.execute(MIGRATE)
                for periode, row in schedules:
                    schedule_id = row.get("id")
                    if not schedule_id:
                        log.warning("Jadwal pada periode %s dilewati karena tidak memiliki id.", periode.get("id"))
                        skipped += 1
                        continue

                    kelas = row.get("class") if isinstance(row.get("class"), dict) else {}
                    psikolog = row.get("user_psikolog") if isinstance(row.get("user_psikolog"), dict) else {}
                    psikolog_detail = psikolog.get("detail") if isinstance(psikolog.get("detail"), dict) else {}

                    cursor.execute(
                        UPSERT,
                        (
                            schedule_id,
                            _to_int(row.get("periode_id") or periode.get("id")),
                            row.get("schedule_name"),
                            row.get("schedule_description"),
                            kelas.get("class_name"),
                            row.get("schedule_execution"),
                            row.get("schedule_price"),
                            psikolog_detail.get("name"),
                            json.dumps(row, ensure_ascii=False),
                        ),
                    )
                    processed += 1
            db.commit()
            log.info(
                "ETL berhasil. Jadwal dari API: %d | Diproses: %d | Dilewati: %d",
                len(schedules), processed, skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data periode ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_periode()


plpbk_periode()

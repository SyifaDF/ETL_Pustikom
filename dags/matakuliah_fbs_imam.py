import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.sdk import dag, task
from dotenv import load_dotenv
from sister_common import HEADERS, get_db_connection

log = logging.getLogger(__name__)

# File .env ada di root project (satu tingkat di atas folder dags).
# override=True: nilai dari .env ini dipakai walaupun sister_common sudah
# memuat /opt/airflow/repo/.env lebih dulu.
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if ENV_FILE.is_file():
    load_dotenv(ENV_FILE, override=True)
else:
    log.warning("File %s tidak ditemukan, memakai environment yang sudah ada.", ENV_FILE)

# Endpoint SIAKAD UNJ (IP internal): {BASE_URL}/{kode_prodi_unj}
# Server Airflow tidak bisa menjangkau https://service-siakad.unj.ac.id (IP publik, timeout)
BASE_URL = "http://192.168.4.30/siakad_api/api/dataDikti/matakuliah/125"

# Daftar kode prodi UNJ (FBS) yang ditarik. Tambahkan kode prodi lain di sini.
KODE_PRODI_LIST = [
    "12016",  # Pendidikan Bahasa dan Sastra Indonesia
]

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS matakuliah_fbs (
        kode_prodi_unj    VARCHAR(10)  NOT NULL,
        kode_mata_kuliah  VARCHAR(20)  NOT NULL,
        nama_mata_kuliah  VARCHAR(255) NOT NULL,
        sks_mata_kuliah   TINYINT UNSIGNED NULL,
        nama_prodi        VARCHAR(255) NULL,
        raw_response      LONGTEXT     NULL,
        updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (kode_prodi_unj, kode_mata_kuliah)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# Insert baru, atau update jika (kode_prodi_unj, kode_mata_kuliah) sudah ada
QUERY = """
    INSERT INTO matakuliah_fbs (
        kode_prodi_unj, kode_mata_kuliah, nama_mata_kuliah,
        sks_mata_kuliah, nama_prodi, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nama_mata_kuliah = VALUES(nama_mata_kuliah),
        sks_mata_kuliah = VALUES(sks_mata_kuliah),
        nama_prodi = VALUES(nama_prodi),
        raw_response = VALUES(raw_response)
"""


def _to_int(value):
    """Ubah ke integer, kosong atau tidak valid menjadi NULL."""
    try:
        return int(str(value).strip()) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _ambil_matakuliah(kode_prodi: str) -> dict:
    """GET daftar mata kuliah satu prodi dari SIAKAD."""
    url = f"{BASE_URL}/{kode_prodi}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"GET {url} gagal: HTTP {resp.status_code} | {resp.text[:300]}")

    body = resp.json()
    if not body.get("status"):
        raise RuntimeError(f"SIAKAD prodi {kode_prodi}: {body.get('msg')}")
    if not isinstance(body.get("data"), list):
        raise ValueError("Format respons API tidak sesuai. 'data' harus JSON array.")
    return body


def transform(body: dict, kode_prodi: str) -> tuple[list[tuple], int]:
    """Ubah respons API menjadi baris tabel. Return (rows, total_dilewati)."""
    detail = body.get("detail_prodi") or {}
    kode_prodi_unj = (detail.get("kode_prodi_unj") or kode_prodi).strip()
    nama_prodi = (detail.get("nama_prodi") or "").strip() or None

    rows = {}
    skipped = 0
    for row in body["data"]:
        # Kode mata kuliah dari API berisi spasi di belakang ("00051122  ")
        kode_mk = (row.get("kode_mata_kuliah") or "").strip()
        if not kode_mk:
            log.warning("Mata kuliah dilewati karena tidak memiliki kode.")
            skipped += 1
            continue

        # Pakai dict supaya kode ganda dalam satu respons hanya disimpan sekali
        rows[kode_mk] = (
            kode_prodi_unj,
            kode_mk,
            (row.get("nama_mata_kuliah") or "").strip(),
            _to_int(row.get("sks_mata_kuliah")),
            nama_prodi,
            json.dumps(row, ensure_ascii=False),
        )
    return list(rows.values()), skipped


@dag(
    dag_id="matakuliah_fbs",
    schedule="0 3 * * *",  # setiap hari 03:00 UTC, setelah DAG SISTER
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["siakad", "etl", "mariadb"],
)
def matakuliah_fbs():

    @task
    def tarik_dan_simpan_matakuliah():

        # 1. EXTRACT + 2. TRANSFORM per prodi
        all_rows = []
        total_data = 0
        total_skipped = 0
        for kode_prodi in KODE_PRODI_LIST:
            body = _ambil_matakuliah(kode_prodi)
            rows, skipped = transform(body, kode_prodi)
            log.info(
                "Prodi %s: %d mata kuliah dari API, %d valid, %d dilewati.",
                kode_prodi, len(body["data"]), len(rows), skipped,
            )
            total_data += len(body["data"])
            total_skipped += skipped
            all_rows.extend(rows)

        if not all_rows:
            log.warning("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi
        db = None
        try:
            log.info(
                "Koneksi MariaDB: %s:%s/%s (user %s)",
                os.getenv("DB_HOST"), os.getenv("DB_PORT"),
                os.getenv("DB_NAME"), os.getenv("DB_USER"),
            )
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.execute(CREATE_TABLE)
                cursor.executemany(QUERY, all_rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(all_rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data mata kuliah ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_matakuliah()


matakuliah_fbs()

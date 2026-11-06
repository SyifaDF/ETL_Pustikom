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

# Endpoint pemakaian barang (IP internal): {BASE_URL}/{kode_satker}
BASE_URL = "http://10.255.1.38/item-usages"

# Daftar kode satker yang ditarik. Tambahkan kode satker lain di sini.
KODE_SATKER_LIST = [
    "999009",  # Fakultas Pendidikan Psikologi
]

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS pemakaian_barang_satker (
        kode_satker   VARCHAR(20)  NOT NULL,
        nama_satker   VARCHAR(255) NOT NULL,
        alias_satker  VARCHAR(50)  NULL,
        kode_unit     VARCHAR(20)  NULL,
        updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (kode_satker)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

# Insert baru, atau update jika kode_satker sudah ada
QUERY = """
    INSERT INTO pemakaian_barang_satker (
        kode_satker, nama_satker, alias_satker, kode_unit
    )
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nama_satker = VALUES(nama_satker),
        alias_satker = VALUES(alias_satker),
        kode_unit = VALUES(kode_unit)
"""


def _clean(value):
    """Trim string, kosong menjadi NULL."""
    return (str(value).strip() or None) if value is not None else None


def _ambil_pemakaian(kode_satker: str) -> dict:
    """GET daftar pemakaian barang satu satker."""
    url = f"{BASE_URL}/{kode_satker}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"GET {url} gagal: HTTP {resp.status_code} | {resp.text[:300]}")

    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"Pemakaian barang satker {kode_satker}: {body.get('message')}")
    if not isinstance(body.get("data"), list):
        raise ValueError("Format respons API tidak sesuai. 'data' harus JSON array.")
    return body


def transform(body: dict, kode_satker: str) -> tuple[list[tuple], int]:
    """Ambil data satker (homebase) dari respons API. Return (rows, total_dilewati)."""
    rows = {}
    skipped = 0
    for row in body["data"]:
        homebase = row.get("homebase") or {}
        kd_satker = _clean(homebase.get("kdSatker")) or kode_satker
        nama_satker = _clean(homebase.get("urSatker"))
        if not nama_satker:
            log.warning("Pemakaian %s dilewati karena homebase tidak memiliki nama satker.", row.get("id"))
            skipped += 1
            continue

        # Pakai dict supaya satker yang sama (berulang di tiap pemakaian) hanya disimpan sekali
        rows[kd_satker] = (
            kd_satker,
            nama_satker,
            _clean(homebase.get("alias")),
            _clean(homebase.get("kdUnit")),
        )
    return list(rows.values()), skipped


@dag(
    dag_id="pemakaian_barang",
    schedule="0 3 * * *",  # setiap hari 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["persediaan", "etl", "mariadb"],
)
def pemakaian_barang():

    @task
    def tarik_dan_simpan_satker():

        # 1. EXTRACT + 2. TRANSFORM per satker
        all_rows = {}
        total_data = 0
        total_skipped = 0
        for kode_satker in KODE_SATKER_LIST:
            body = _ambil_pemakaian(kode_satker)
            rows, skipped = transform(body, kode_satker)
            log.info(
                "Satker %s: %d pemakaian dari API, %d satker unik, %d dilewati.",
                kode_satker, len(body["data"]), len(rows), skipped,
            )
            total_data += len(body["data"])
            total_skipped += skipped
            for r in rows:
                all_rows[r[0]] = r

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
                cursor.executemany(QUERY, list(all_rows.values()))
            db.commit()
            log.info(
                "Proses ETL berhasil. Total pemakaian dari API: %d | Satker disimpan: %d | Dilewati: %d",
                total_data, len(all_rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data satker ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_satker()


pemakaian_barang()

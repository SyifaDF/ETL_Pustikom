# ============================================================
# DAG siakad_penjadwalan
# Menarik data penjadwalan kelas dari SIAKAD Lama (AS400) lalu
# menyimpan 3 kolom (id jadwal, kode kelas, kode semester) ke MariaDB.
# ============================================================

import logging
import re
from datetime import datetime, timedelta

import pymysql
import requests
from airflow.sdk import dag, task
from dotenv import dotenv_values

log = logging.getLogger(__name__)


# ============================================================
# KONFIGURASI
# ============================================================

# File .env berada di luar folder dags. Variabel yang dipakai:
#   SIAKAD_LAMA_URL, TOKEN_SIAKAD_LAMA, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
ENV_FILE = "/opt/airflow/repo/.env"

# Kode semester AS400 (bukan kode PDDIKTI) dan kode prodi AS400 yang ditarik
KODE_SEMESTER = "125"
KODE_PRODI = "15126"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "ETL-Pustikom/1.0 (Airflow)",
}

# Pesan SIAKAD Lama bila memang tidak ada data (bukan error)
PESAN_TANPA_DATA = re.compile(r"tidak\s+ada\s+data|tidak\s+ditemukan", re.IGNORECASE)


def baca_env(*nama_wajib: str) -> dict:
    """Baca .env saat task berjalan dan pastikan variabel wajib terisi."""
    env = {k: (v or "").strip() for k, v in dotenv_values(ENV_FILE).items()}
    kosong = [nama for nama in nama_wajib if not env.get(nama)]
    if kosong:
        raise ValueError(f"Variabel berikut belum diisi di {ENV_FILE}: {', '.join(kosong)}")
    return env


# ============================================================
# API SIAKAD LAMA
# ============================================================

def get_penjadwalan(kode_semester: str, kode_prodi: str) -> list:
    """
    GET {SIAKAD_LAMA_URL}/penjadwalan/{semester}/{prodi}/{token}
    Token SIAKAD Lama ditaruh sebagai segmen path terakhir, BUKAN header Authorization.
    Mengembalikan isi respons (list jadwal).
    """
    env = baca_env("SIAKAD_LAMA_URL", "TOKEN_SIAKAD_LAMA")
    endpoint = f"penjadwalan/{kode_semester}/{kode_prodi}"
    url = f"{env['SIAKAD_LAMA_URL'].rstrip('/')}/{endpoint}/{env['TOKEN_SIAKAD_LAMA']}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=90)
    except requests.RequestException as e:
        # Pesan asli requests memuat URL lengkap (beserta token), jadi tidak diteruskan ke log
        raise RuntimeError(f"GET {endpoint} gagal: {type(e).__name__}") from None
    if not resp.ok:
        raise RuntimeError(f"GET {endpoint} gagal: HTTP {resp.status_code}")

    # Bentuk respons: {"status": true, "isi": [...]} atau {"status": false, "msg": "..."}
    body = resp.json()
    if body.get("status") and isinstance(body.get("isi"), list):
        return body["isi"]

    pesan = str(body.get("pesan") or body.get("msg") or "").strip()
    if PESAN_TANPA_DATA.search(pesan):
        return []
    raise RuntimeError(f"GET {endpoint} ditolak SIAKAD Lama: {pesan or 'respons tidak berisi data'}")


# ============================================================
# MARIADB
# ============================================================

def get_db_connection():
    """Koneksi MariaDB dari .env (transaksi dikontrol manual)."""
    env = baca_env("DB_NAME", "DB_USER", "DB_PASSWORD")
    return pymysql.connect(
        host=env.get("DB_HOST") or "localhost",
        port=int(env.get("DB_PORT") or 3306),
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_NAME"],
        charset="utf8mb4",
        autocommit=False,
    )


# id_jadwal dari API dijadikan PRIMARY KEY, supaya ON DUPLICATE KEY UPDATE
# memperbarui jadwal yang sama dan baris kembar dari API tidak tersimpan dua kali.
CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS siakad_penjadwalan (
        id_jadwal     INT          NOT NULL PRIMARY KEY,
        kode_kelas    VARCHAR(20)  NULL,
        kode_semester VARCHAR(5)   NULL COMMENT 'field kodsks dari API = kode semester AS400'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

UPSERT = """
    INSERT INTO siakad_penjadwalan (id_jadwal, kode_kelas, kode_semester)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        kode_kelas = VALUES(kode_kelas),
        kode_semester = VALUES(kode_semester)
"""


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="siakad_penjadwalan",
    schedule="0 2 * * *",  # setiap hari 02:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["siakad", "etl", "mariadb"],
)
def siakad_penjadwalan():

    @task
    def buat_tabel():
        db = get_db_connection()
        try:
            with db.cursor() as cursor:
                cursor.execute(CREATE_TABLE)
            db.commit()
            log.info("Tabel siakad_penjadwalan siap.")
        finally:
            db.close()

    @task
    def tarik_dan_simpan_penjadwalan():
        # 1. EXTRACT
        data = get_penjadwalan(KODE_SEMESTER, KODE_PRODI)
        log.info("Berhasil mengambil %d data penjadwalan dari SIAKAD Lama.", len(data))
        if not data:
            return

        # 2. TRANSFORM: ambil 3 kolom, lewati baris tanpa id
        baris = [
            (int(row["id"]), row.get("kelas") or None, row.get("kodsks") or None)
            for row in data
            if str(row.get("id") or "").isdigit()
        ]
        jumlah_dilewati = len(data) - len(baris)
        if jumlah_dilewati:
            log.warning("%d data penjadwalan dilewati karena id kosong/tidak valid.", jumlah_dilewati)

        # 3. LOAD: satu transaksi
        db = get_db_connection()
        try:
            with db.cursor() as cursor:
                cursor.executemany(UPSERT, baris)
            db.commit()
        except Exception:
            db.rollback()
            log.exception("Gagal menyimpan data penjadwalan ke MariaDB.")
            raise
        finally:
            db.close()

        log.info(
            "Proses ETL selesai. Dari API: %d | Diproses: %d | Dilewati: %d | id jadwal unik: %d",
            len(data), len(baris), jumlah_dilewati, len({b[0] for b in baris}),
        )

    buat_tabel() >> tarik_dan_simpan_penjadwalan()


siakad_penjadwalan()
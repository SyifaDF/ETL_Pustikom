import json
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import requests
from airflow.sdk import dag, task
from sister_common import get_db_connection

log = logging.getLogger(__name__)

# Konfigurasi dibaca dari environment variable proses Airflow (os.getenv):
#   E_ASET_BASE_URL, E_ASET_AUTH_EMAIL, E_ASET_AUTH_PASSWORD,
#   E_ASET_DEVICE_NAME (opsional, default "integrasi-unj")
# Dibaca saat task berjalan, bukan saat DAG di-parse.

API_PREFIX = "/api/v1"
PER_PAGE = 100  # jumlah data yang diambil (maksimal yang diterima API: 100)

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
}

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS e_aset_daftar_aset (
        id                   VARCHAR(64)   NOT NULL PRIMARY KEY,
        kd_upb               VARCHAR(32),
        unit_pengelola       VARCHAR(255),
        kd_brg_bmn           VARCHAR(32),
        nup_bmn              VARCHAR(32),
        kd_brg_bmu           VARCHAR(32),
        nup                  VARCHAR(32),
        ur_kelompok          VARCHAR(255),
        nama_barang          VARCHAR(255),
        kondisi              VARCHAR(50),
        umur_aset            INT,
        intra_extra          VARCHAR(20),
        tanggal_perolehan    DATE,
        nilai_perolehan      DECIMAL(20,2),
        nilai_susut_awal     DECIMAL(20,2),
        nilai_buku           DECIMAL(20,2),
        mutasi               VARCHAR(32),
        akumulasi_penyusutan DECIMAL(20,2),
        nilai_buku_neraca    DECIMAL(20,2),
        henti_guna           TINYINT(1),
        usul_hapus           TINYINT(1),
        tanggal_penghapusan  DATE,
        is_active            TINYINT(1),
        raw_response         LONGTEXT,
        synced_at            DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_kd_upb (kd_upb),
        KEY idx_kd_brg_bmu (kd_brg_bmu)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
"""

COLUMNS = (
    "id", "kd_upb", "unit_pengelola", "kd_brg_bmn", "nup_bmn", "kd_brg_bmu", "nup",
    "ur_kelompok", "nama_barang", "kondisi", "umur_aset", "intra_extra",
    "tanggal_perolehan", "nilai_perolehan", "nilai_susut_awal", "nilai_buku", "mutasi",
    "akumulasi_penyusutan", "nilai_buku_neraca", "henti_guna", "usul_hapus",
    "tanggal_penghapusan", "is_active", "raw_response",
)

# Insert baru, atau update jika id sudah ada
QUERY = f"""
    INSERT INTO e_aset_daftar_aset ({", ".join(COLUMNS)})
    VALUES ({", ".join(["%s"] * len(COLUMNS))})
    ON DUPLICATE KEY UPDATE
        {", ".join(f"{c} = VALUES({c})" for c in COLUMNS if c != "id")}
"""


# ---------------------------------------------------------------- API helpers

def _base_url() -> str:
    base_url = os.getenv("E_ASET_BASE_URL", "").rstrip("/")
    if not base_url:
        raise ValueError("E_ASET_BASE_URL belum diisi di .env")
    if not base_url.endswith(API_PREFIX):
        base_url += API_PREFIX
    return base_url


def login(session: requests.Session, base_url: str) -> str:
    """POST /auth/token, return bearer token."""
    missing = [k for k in ("E_ASET_AUTH_EMAIL", "E_ASET_AUTH_PASSWORD") if not os.getenv(k)]
    if missing:
        raise ValueError("Kredensial e-Aset belum lengkap di .env: " + ", ".join(missing))

    resp = session.post(
        f"{base_url}/auth/token",
        json={
            "email": os.getenv("E_ASET_AUTH_EMAIL"),
            "password": os.getenv("E_ASET_AUTH_PASSWORD"),
            "device_name": os.getenv("E_ASET_DEVICE_NAME", "integrasi-unj"),
        },
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Login e-Aset gagal: HTTP {resp.status_code} | {resp.text[:300]}")

    body = resp.json()
    token = (body.get("data") or {}).get("token")
    if not body.get("success") or not token:
        raise ValueError(f"Login e-Aset tidak mengembalikan token: {body.get('message')}")
    return token


def fetch_aset() -> list[dict]:
    """Login, lalu GET /aset sekali (hanya halaman pertama, PER_PAGE data)."""
    base_url = _base_url()
    with requests.Session() as session:
        session.headers.update(HEADERS)
        session.headers["Authorization"] = f"Bearer {login(session, base_url)}"

        resp = session.get(f"{base_url}/aset", params={"per_page": PER_PAGE}, timeout=60)
        if not resp.ok:
            raise RuntimeError(f"GET /aset gagal: HTTP {resp.status_code} | {resp.text[:300]}")

        body = resp.json()
        if not body.get("success") or not isinstance(body.get("data"), list):
            raise ValueError(f"Format respons /aset tidak sesuai: {body.get('message')}")

        meta = body.get("meta") or {}
        log.info(
            "GET /aset: %d data diambil (total di API: %s, hanya halaman pertama).",
            len(body["data"]), meta.get("total"),
        )
        return body["data"]


# ----------------------------------------------------------- transform helpers

def _to_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_decimal(value):
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except InvalidOperation:
        return None


def _to_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _to_bool(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "ya", "yes")
    return bool(value)


def _transform(row: dict) -> tuple:
    return (
        row.get("id"),
        row.get("kd_upb"),
        row.get("unit_pengelola"),
        row.get("kd_brg_bmn"),
        row.get("nup_bmn"),
        row.get("kd_brg_bmu"),
        row.get("nup"),
        row.get("ur_kelompok"),
        row.get("nama_barang"),
        row.get("kondisi"),
        _to_int(row.get("umur_aset")),
        row.get("intra_extra"),
        _to_date(row.get("tanggal_perolehan")),
        _to_decimal(row.get("nilai_perolehan")),
        _to_decimal(row.get("nilai_susut_awal")),
        _to_decimal(row.get("nilai_buku")),
        row.get("mutasi"),
        _to_decimal(row.get("akumulasi_penyusutan")),
        _to_decimal(row.get("nilai_buku_neraca")),
        _to_bool(row.get("henti_guna")),
        _to_bool(row.get("usul_hapus")),
        _to_date(row.get("tanggal_penghapusan")),
        _to_bool(row.get("is_active")),
        json.dumps(row, ensure_ascii=False),
    )


# ------------------------------------------------------------------------- DAG

@dag(
    dag_id="e_aset_daftar_aset",
    schedule="0 3 * * *",  # setiap hari 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["e-aset", "etl", "mariadb"],
)
def e_aset_daftar_aset():

    @task
    def tarik_dan_simpan_aset():
        # 1. EXTRACT
        data = fetch_aset()
        total_data = len(data)
        log.info("Berhasil mengambil %d data aset dari e-Aset.", total_data)
        if total_data == 0:
            log.info("Tidak ada data aset untuk disimpan.")
            return

        # 2. TRANSFORM
        rows, total_skipped = [], 0
        for row in data:
            if not row.get("id"):
                log.warning("Data aset dilewati karena tidak memiliki ID.")
                total_skipped += 1
                continue
            rows.append(_transform(row))

        if not rows:
            log.warning("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi
        db = None
        try:
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.execute(CREATE_TABLE)
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data aset ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_aset()


e_aset_daftar_aset()

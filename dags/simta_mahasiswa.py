import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from simta_common import SimtaApi
from sister_common import get_db_connection

log = logging.getLogger(__name__)

# Daftar kode prodi yang diambil. Token SIMTA harus punya ability untuk
# setiap kode di sini (prodi:{kode} atau prodi:*), kalau tidak API membalas 403.
PRODI_LIST = ["05106"]

# Jumlah baris per halaman (maksimum 1000 menurut dokumentasi SIMTA).
PER_PAGE = 1000

CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS simta_mahasiswa (
        id                 BIGINT        NOT NULL,
        nim                VARCHAR(50)   NULL,
        nama               VARCHAR(255)  NULL,
        angkatan           INT           NULL,
        kode_prodi         VARCHAR(20)   NULL,
        status_mahasiswa   VARCHAR(50)   NULL,
        status_kualifikasi VARCHAR(50)   NULL,
        semester_sinkron   VARCHAR(20)   NULL,
        created_at         DATETIME      NULL,
        updated_at         DATETIME      NULL,
        synced_at          DATETIME      NOT NULL,
        raw_response       LONGTEXT      NULL,
        PRIMARY KEY (id),
        KEY idx_simta_mhs_prodi (kode_prodi),
        KEY idx_simta_mhs_nim (nim)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

QUERY = """
    INSERT INTO simta_mahasiswa (
        id, nim, nama, angkatan, kode_prodi, status_mahasiswa,
        status_kualifikasi, semester_sinkron, created_at, updated_at,
        synced_at, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nim = VALUES(nim),
        nama = VALUES(nama),
        angkatan = VALUES(angkatan),
        kode_prodi = VALUES(kode_prodi),
        status_mahasiswa = VALUES(status_mahasiswa),
        status_kualifikasi = VALUES(status_kualifikasi),
        semester_sinkron = VALUES(semester_sinkron),
        created_at = VALUES(created_at),
        updated_at = VALUES(updated_at),
        synced_at = VALUES(synced_at),
        raw_response = VALUES(raw_response)
"""


def _to_datetime(value):
    """Ubah teks tanggal ISO 8601 menjadi DATETIME UTC (tanpa timezone)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _to_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


@dag(
    dag_id="simta_mahasiswa",
    schedule="0 4 * * *",  # setiap hari 04:00 UTC (11:00 WIB)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["simta", "etl", "mariadb"],
)
def simta_mahasiswa():

    @task
    def tarik_dan_simpan_mahasiswa():
        synced_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

        # 1. EXTRACT: semua halaman, semua prodi (ikuti links.next)
        rows = []
        skipped = 0
        with SimtaApi() as api:
            for kode_prodi in PRODI_LIST:
                jumlah_prodi = 0
                path = f"api/etl/prodi/{kode_prodi}/mahasiswa"
                for row in api.iter_pages(path, params={"per_page": PER_PAGE}):
                    mhs_id = row.get("id")
                    if not mhs_id:
                        log.warning("Data mahasiswa dilewati karena tidak memiliki id (prodi %s).", kode_prodi)
                        skipped += 1
                        continue
                    rows.append((
                        mhs_id,
                        row.get("nim"),
                        row.get("nama"),
                        _to_int(row.get("angkatan")),
                        row.get("kode_prodi"),
                        row.get("status_mahasiswa"),
                        row.get("status_kualifikasi"),
                        row.get("semester_sinkron"),
                        _to_datetime(row.get("created_at")),
                        _to_datetime(row.get("updated_at")),
                        synced_at,
                        json.dumps(row, ensure_ascii=False),
                    ))
                    jumlah_prodi += 1
                log.info("Prodi %s: %d mahasiswa diambil.", kode_prodi, jumlah_prodi)

        log.info("Total mahasiswa dari API: %d | Dilewati: %d", len(rows), skipped)
        if not rows:
            log.warning("Tidak ada data mahasiswa untuk disimpan. Database tidak diubah.")
            return

        # 2. LOAD: satu transaksi
        db = None
        try:
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.execute(CREATE_TABLE)  # buat tabel jika belum ada
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info("ETL SIMTA berhasil. Disimpan/diperbarui: %d baris.", len(rows))
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data mahasiswa ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_mahasiswa()


simta_mahasiswa()

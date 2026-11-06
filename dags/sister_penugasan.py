import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from sister_common import get_db_connection, get_json

log = logging.getLogger(__name__)

# ID SDM dosen yang datanya diambil dari SISTER
ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"

# Insert baru, atau update jika id sudah ada
QUERY = """
    INSERT INTO sister_penugasan (
        id, id_sdm, status_kepegawaian, ikatan_kerja, jenjang_pendidikan,
        unit_kerja, perguruan_tinggi, tanggal_mulai, tanggal_keluar,
        apakah_penugasan_homebase, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        id_sdm = VALUES(id_sdm),
        status_kepegawaian = VALUES(status_kepegawaian),
        ikatan_kerja = VALUES(ikatan_kerja),
        jenjang_pendidikan = VALUES(jenjang_pendidikan),
        unit_kerja = VALUES(unit_kerja),
        perguruan_tinggi = VALUES(perguruan_tinggi),
        tanggal_mulai = VALUES(tanggal_mulai),
        tanggal_keluar = VALUES(tanggal_keluar),
        apakah_penugasan_homebase = VALUES(apakah_penugasan_homebase),
        raw_response = VALUES(raw_response)
"""


def _to_date(value):
    """Ubah teks tanggal (YYYY-MM-DD) menjadi date, gagal menjadi NULL."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


@dag(
    dag_id="sister_penugasan",
    schedule="0 3 * * *",  # 30 menit setelah sister_publikasi (UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["sister", "etl", "mariadb"],
)
def sister_penugasan():

    @task
    def tarik_dan_simpan_penugasan():

        # 1. EXTRACT: login lalu GET /penugasan
        data = get_json("penugasan", {"id_sdm": ID_SDM})

        if not isinstance(data, list):
            raise ValueError("Format respons API tidak sesuai. Diharapkan JSON array.")

        total_data = len(data)
        log.info("Berhasil mengambil %d data penugasan dari SISTER.", total_data)
        if total_data == 0:
            log.info("Tidak ada data penugasan untuk disimpan.")
            return

        # 2. TRANSFORM: rapikan tipe data tiap baris
        rows = []
        total_skipped = 0
        for row in data:
            penugasan_id = row.get("id")
            if not penugasan_id:
                log.warning("Data penugasan dilewati karena tidak memiliki ID.")
                total_skipped += 1
                continue

            tanggal_mulai = _to_date(row.get("tanggal_mulai"))
            if row.get("tanggal_mulai") and tanggal_mulai is None:
                log.warning("Format tanggal_mulai tidak dikenali (id=%s), disimpan NULL.", penugasan_id)

            tanggal_keluar = _to_date(row.get("tanggal_keluar"))
            if row.get("tanggal_keluar") and tanggal_keluar is None:
                log.warning("Format tanggal_keluar tidak dikenali (id=%s), disimpan NULL.", penugasan_id)

            rows.append((
                penugasan_id,
                ID_SDM,
                row.get("status_kepegawaian"),
                row.get("ikatan_kerja"),
                row.get("jenjang_pendidikan"),
                row.get("unit_kerja"),
                row.get("perguruan_tinggi"),
                tanggal_mulai,
                tanggal_keluar,
                row.get("apakah_penugasan_homebase"),
                json.dumps(row, ensure_ascii=False),
            ))

        if not rows:
            log.warning("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi
        db = None
        try:
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data penugasan ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_penugasan()


sister_penugasan()

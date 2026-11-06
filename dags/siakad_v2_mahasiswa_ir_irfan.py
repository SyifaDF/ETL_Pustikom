"""
DAG siakad_v2_mahasiswa_ir_irfan

Menarik data mahasiswa dari SIAKAD v2 (AS400) lalu menyimpannya ke MariaDB.

Alur:
    1. buat_tabel     -> buat tabel siakad_mahasiswa bila belum ada
    2. daftar_prodi   -> ambil daftar kode prodi (atau pakai param kode_prodi)
    3. tarik_dan_simpan_mahasiswa (1 task per prodi)
                      -> GET dataMahasiswaPerProdiAngkatan untuk angkatan
                         ANGKATAN_AWAL s.d. ANGKATAN_AKHIR, lalu upsert ke MariaDB
                         berdasarkan NIM

Konfigurasi dibaca dari .env server (lihat .env.example di root repo):
    SIAKAD_LAMA_URL, TOKEN_SIAKAD_LAMA, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import json
import logging
import re
from datetime import datetime, timedelta

import pymysql
import requests
from airflow.sdk import Param, dag, get_current_context, task
from dotenv import dotenv_values

log = logging.getLogger(__name__)


# ============================================================
# 1. Konfigurasi
# ============================================================

# File .env berada di luar folder dags
ENV_PATH = "/opt/airflow/repo/.env"

# Rentang angkatan yang ditarik (keduanya ikut)
ANGKATAN_AWAL = 2020
ANGKATAN_AKHIR = 2020

# Kode semester AS400 bawaan (bukan kode PDDIKTI). Bisa diganti lewat param DAG.
SEMESTER_DEFAULT = "125"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "ETL-Pustikom/1.0 (Airflow)",
}

# Pesan SIAKAD Lama bila prodi/angkatan memang tidak punya data (bukan error)
PESAN_TANPA_DATA = re.compile(
    r"tidak\s+ada\s+data|data\s+tidak\s+ditemukan|tidak\s+ditemukan\s+data",
    re.IGNORECASE,
)


def baca_env(*nama_wajib: str) -> dict:
    """
    Baca file .env dan pastikan variabel wajib terisi.
    File dibaca ulang setiap dipanggil, jadi token yang diperbarui di .env
    langsung terpakai pada run berikutnya tanpa restart Airflow.
    """
    env = {k: (v or "").strip() for k, v in dotenv_values(ENV_PATH).items()}
    kosong = [nama for nama in nama_wajib if not env.get(nama)]
    if kosong:
        raise ValueError(f"Variabel berikut belum diisi di {ENV_PATH}: {', '.join(kosong)}")
    return env


# ============================================================
# 2. Akses API SIAKAD v2 (AS400)
# ============================================================

def get_siakad_config() -> tuple[str, str]:
    """Return (base_url, token) SIAKAD Lama dari .env."""
    env = baca_env("SIAKAD_LAMA_URL", "TOKEN_SIAKAD_LAMA")
    return env["SIAKAD_LAMA_URL"].rstrip("/"), env["TOKEN_SIAKAD_LAMA"]


def parse_json_longgar(text: str):
    """
    Ubah teks respons menjadi JSON.
    SIAKAD Lama kadang menyisipkan teks lain (mis. warning PHP) sebelum JSON,
    jadi bila gagal, coba lagi mulai dari setiap karakter '{' atau '['.
    """
    text = text.strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except ValueError:
        pass

    for awal_json in re.finditer(r"[\[{]", text[:20000]):
        try:
            return json.loads(text[awal_json.start():])
        except ValueError:
            continue
    return None


def ambil_isi_respons(body, endpoint: str) -> list:
    """
    Ambil daftar data dari respons SIAKAD Lama.
    Bentuk respons yang dikenali:
        [ ... ]                                       -> langsung dipakai
        {"status": true,  "isi": [ ... ]}             -> isi dipakai
        {"status": false, "pesan": "Tidak ditemukan"} -> dianggap kosong
        {"status": 401,   "msg": "..."}               -> token ditolak
    """
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        raise ValueError(f"GET {endpoint}: format respons tidak dikenali ({type(body).__name__})")

    pesan = body.get("pesan") or body.get("msg") or ""
    pesan = pesan if isinstance(pesan, str) else ""

    if str(body.get("status")) == "401":
        raise PermissionError(
            f"GET {endpoint}: token ditolak ({pesan or 'Unauthorized'}). "
            f"Perbarui TOKEN_SIAKAD_LAMA di {ENV_PATH} lewat POST /api/as400/signin."
        )

    isi = body.get("isi", body.get("data"))
    if body.get("status") and isinstance(isi, list):
        return isi

    if PESAN_TANPA_DATA.search(pesan) or body.get("pesan") == [] or body.get("msg") == []:
        return []

    raise RuntimeError(f"GET {endpoint}: {pesan or 'respons tidak berisi data'}")


def get_siakad(base_url: str, endpoint: str, token: str | None = None) -> list:
    """
    GET endpoint AS400 dan kembalikan datanya sebagai list.
    Token ditaruh sebagai segmen path terakhir, sesuai aturan SIAKAD Lama:
        {base_url}/{endpoint}/{token}
    Pesan error hanya memuat endpoint, supaya token tidak tercetak di log.
    """
    endpoint = endpoint.strip("/")
    url = f"{base_url}/{endpoint}/{token}" if token else f"{base_url}/{endpoint}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=90)
    except requests.RequestException as e:
        # Pesan asli requests memuat URL lengkap (beserta token), jadi tidak diteruskan
        raise RuntimeError(f"GET {endpoint} gagal: {type(e).__name__}") from None

    if not resp.ok:
        raise RuntimeError(f"GET {endpoint} gagal: HTTP {resp.status_code}")

    if not resp.text.strip():
        return []

    body = parse_json_longgar(resp.text)
    if body is None:
        raise ValueError(f"GET {endpoint}: respons bukan JSON ({resp.text[:120]!r})")

    return ambil_isi_respons(body, endpoint)


# ============================================================
# 3. Akses MariaDB
# ============================================================

def get_db_connection():
    """Koneksi MariaDB dari .env (transaksi dikontrol manual)."""
    env = baca_env("DB_NAME", "DB_USER", "DB_PASSWORD")
    try:
        return pymysql.connect(
            host=env.get("DB_HOST") or "localhost",
            port=int(env.get("DB_PORT") or 3306),
            user=env["DB_USER"],
            password=env["DB_PASSWORD"],
            database=env["DB_NAME"],
            charset="utf8mb4",
            autocommit=False,
        )
    except pymysql.MySQLError as e:
        raise RuntimeError(f"Gagal konek MariaDB: {type(e).__name__} {e.args}") from None


# Dijalankan task buat_tabel. Tabel yang sudah ada tidak diubah, jadi perubahan
# struktur (mis. kolom baru) tetap harus lewat ALTER TABLE manual.
CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS siakad_mahasiswa (
        nim               VARCHAR(20)   NOT NULL PRIMARY KEY,
        nama              VARCHAR(255)  NOT NULL,
        kode_prodi        VARCHAR(10)   NULL,
        nama_prodi        VARCHAR(255)  NULL,
        nama_fakultas     VARCHAR(255)  NULL,
        jenjang           VARCHAR(10)   NULL,
        angkatan          SMALLINT      NULL,
        semester_masuk    VARCHAR(10)   NULL COMMENT 'kode semester AS400',
        semester_ke       SMALLINT      NULL,
        jenis_daftar      VARCHAR(10)   NULL,
        kelamin           CHAR(1)       NULL,
        tempat_lahir      VARCHAR(100)  NULL,
        tanggal_lahir     DATE          NULL,
        email             VARCHAR(255)  NULL,
        hp                VARCHAR(30)   NULL,
        tanggal_masuk     DATE          NULL,
        kode_masuk        VARCHAR(10)   NULL,
        terdaftar         VARCHAR(100)  NULL COMMENT 'status registrasi pada semester_berjalan',
        kode_status       VARCHAR(5)    NULL,
        status_mahasiswa  VARCHAR(50)   NULL,
        tanggal_keluar    DATE          NULL,
        ipk               DECIMAL(4,2)  NULL,
        total_sks         SMALLINT      NULL,
        nidn_dosen_pa     VARCHAR(30)   NULL,
        nama_dosen_pa     VARCHAR(255)  NULL,
        semester_berjalan VARCHAR(10)   NOT NULL COMMENT 'kode semester AS400 saat ditarik',
        raw_response      LONGTEXT      NULL CHECK (JSON_VALID(raw_response)),
        updated_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_siakad_mahasiswa_prodi_angkatan (kode_prodi, angkatan)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


# ============================================================
# 4. Transform: satu data API -> satu baris tabel
# ============================================================

def _to_int(value):
    """Ubah ke integer, kosong atau tidak valid menjadi NULL."""
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_float(value):
    """Ubah teks angka (mis. "3.88") menjadi float, kosong atau tidak valid menjadi NULL."""
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_date(value):
    """Ubah teks tanggal (YYYY-MM-DD) menjadi date, gagal menjadi NULL."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _teks(value):
    """Teks kosong disimpan sebagai NULL."""
    return value or None


def ubah_ke_baris(mhs: dict, semester: str) -> dict:
    """Petakan satu data mahasiswa dari API ke kolom tabel siakad_mahasiswa."""
    status = mhs.get("statusMahasiswa") or {}
    dosen_pa = mhs.get("dosenPembimbing") or {}

    return {
        "nim":               mhs["nim"],
        "nama":              mhs.get("namaLengkap") or mhs.get("nama") or "",
        "kode_prodi":        _teks(mhs.get("kodeProdi")),
        "nama_prodi":        _teks(mhs.get("namaProdi")),
        "nama_fakultas":     _teks(mhs.get("namaFakultas")),
        "jenjang":           _teks(mhs.get("jenjang")),
        "angkatan":          _to_int(mhs.get("angkatan")),
        "semester_masuk":    _teks(mhs.get("smsMasuk")),
        "semester_ke":       _to_int(mhs.get("semester_ke")),
        "jenis_daftar":      _teks(mhs.get("jenisDaftar")),
        "kelamin":           _teks(mhs.get("kelamin")),
        "tempat_lahir":      _teks(mhs.get("tempatLahir")),
        "tanggal_lahir":     _to_date(mhs.get("tanggalLahir")),
        "email":             _teks(mhs.get("email")),
        "hp":                _teks(mhs.get("hpm") or mhs.get("telp")),
        "tanggal_masuk":     _to_date(mhs.get("tanggalMasuk")),
        "kode_masuk":        _teks(mhs.get("kodeMasuk")),
        "terdaftar":         _teks(mhs.get("terdaftar")),
        "kode_status":       _teks(status.get("kode")),
        "status_mahasiswa":  _teks(status.get("status")),
        "tanggal_keluar":    _to_date(status.get("tanggalKeluarSK") or status.get("tanggalKeluar")),
        "ipk":               _to_float(mhs.get("ipk")),
        "total_sks":         _to_int(mhs.get("totalSKS")),
        "nidn_dosen_pa":     _teks(dosen_pa.get("nidn")),
        "nama_dosen_pa":     _teks(dosen_pa.get("nama")),
        "semester_berjalan": semester,
        "raw_response":      json.dumps(mhs, ensure_ascii=False),
    }


# Daftar kolom diambil dari ubah_ke_baris, jadi query selalu sejalan dengan pemetaan di atas
KOLOM = list(ubah_ke_baris({"nim": ""}, "").keys())

# Insert baru, atau update semua kolom (kecuali nim) jika nim sudah ada
UPSERT = (
    f"INSERT INTO siakad_mahasiswa ({', '.join(KOLOM)}) "
    f"VALUES ({', '.join(['%(' + k + ')s' for k in KOLOM])}) "
    "ON DUPLICATE KEY UPDATE "
    + ", ".join(f"{k} = VALUES({k})" for k in KOLOM if k != "nim")
)


# ============================================================
# 5. DAG
# ============================================================

@dag(
    dag_id="siakad_v2_mahasiswa_ir_irfan",
    schedule="0 3 * * *",  # setiap hari 03:00 UTC, setelah DAG SISTER
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        # Kode semester AS400 (bukan kode PDDIKTI), mis. "125"
        "semester": Param(SEMESTER_DEFAULT, type="string"),
        # Kosong = semua prodi; isi untuk membatasi, mis. ["15126"]
        "kode_prodi": Param([], type="array"),
    },
    tags=["siakad", "etl", "mariadb"],
)
def siakad_v2_mahasiswa_ir_irfan():

    @task
    def buat_tabel():
        """Buat tabel siakad_mahasiswa bila belum ada (user DB butuh hak CREATE)."""
        db = get_db_connection()
        try:
            with db.cursor() as cursor:
                cursor.execute(CREATE_TABLE)
            db.commit()
            log.info("Tabel siakad_mahasiswa siap.")
        finally:
            db.close()

    @task
    def daftar_prodi() -> list[str]:
        """Ambil kode prodi AS400 yang akan ditarik."""
        kode_dari_param = get_current_context()["params"].get("kode_prodi") or []
        if kode_dari_param:
            log.info("Memakai %d prodi dari parameter DAG.", len(kode_dari_param))
            return [str(kode) for kode in kode_dari_param]

        base_url, token = get_siakad_config()
        try:
            semua_prodi = get_siakad(base_url, "programStudi/All")
        except (RuntimeError, ValueError):
            # Sebagian server SIAKAD Lama mewajibkan token juga di endpoint ini
            semua_prodi = get_siakad(base_url, "programStudi/All", token)

        kode_prodi = sorted({str(p["kodeProdi"]) for p in semua_prodi if p.get("kodeProdi")})
        if not kode_prodi:
            raise ValueError("Daftar prodi dari SIAKAD Lama kosong.")
        log.info("Berhasil mengambil %d prodi dari SIAKAD Lama.", len(kode_prodi))
        return kode_prodi

    # Maksimal 4 prodi berjalan bersamaan, supaya server AS400 tidak kebanjiran request
    @task(max_active_tis_per_dag=4)
    def tarik_dan_simpan_mahasiswa(kode_prodi: str):
        semester = get_current_context()["params"]["semester"]
        base_url, token = get_siakad_config()

        # 1. EXTRACT: satu request per angkatan
        data = []
        angkatan_rusak = []
        for angkatan in range(ANGKATAN_AWAL, ANGKATAN_AKHIR + 1):
            endpoint = f"dataMahasiswaPerProdiAngkatan/{kode_prodi}/{angkatan}/{semester}"
            try:
                mahasiswa = get_siakad(base_url, endpoint, token)
            except ValueError as e:
                # Server AS400 kadang gagal meng-encode datanya sendiri dan membalas teks
                # (mis. "Malformed UTF-8 characters"). Angkatan ini dilewati supaya angkatan
                # lain di prodi yang sama tetap tersimpan. Error token/koneksi tetap menggagalkan task.
                log.warning("Prodi %s angkatan %d dilewati: %s", kode_prodi, angkatan, e)
                angkatan_rusak.append(angkatan)
                continue
            if mahasiswa:
                log.info("Prodi %s angkatan %d: %d mahasiswa.", kode_prodi, angkatan, len(mahasiswa))
            data.extend(mahasiswa)

        log.info("Prodi %s: total %d mahasiswa dari SIAKAD Lama.", kode_prodi, len(data))
        if not data:
            return

        # 2. TRANSFORM: data tanpa NIM dilewati
        baris = [ubah_ke_baris(mhs, semester) for mhs in data if mhs.get("nim")]
        jumlah_dilewati = len(data) - len(baris)
        if jumlah_dilewati:
            log.warning("Prodi %s: %d data dilewati karena tidak memiliki NIM.", kode_prodi, jumlah_dilewati)
        if not baris:
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi per prodi
        db = get_db_connection()
        try:
            with db.cursor() as cursor:
                cursor.executemany(UPSERT, baris)
            db.commit()
        except Exception:
            db.rollback()
            log.exception("Gagal menyimpan data mahasiswa prodi %s ke MariaDB.", kode_prodi)
            raise
        finally:
            db.close()

        log.info(
            "Prodi %s selesai. Dari API: %d | Disimpan: %d | Dilewati: %d",
            kode_prodi, len(data), len(baris), jumlah_dilewati,
        )
        if angkatan_rusak:
            log.warning(
                "Prodi %s: angkatan %s tidak tersimpan karena respons SIAKAD Lama rusak.",
                kode_prodi, angkatan_rusak,
            )

    # Urutan: buat tabel -> ambil daftar prodi -> tarik & simpan per prodi (paralel)
    prodi = daftar_prodi()
    buat_tabel() >> prodi
    tarik_dan_simpan_mahasiswa.expand(kode_prodi=prodi)


siakad_v2_mahasiswa_ir_irfan()

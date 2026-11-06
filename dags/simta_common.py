"""Akses ke SIMTA API (Laravel). Token Bearer statis, paginasi cursor.

Konfigurasi dibaca dari /opt/airflow/repo/.env (lihat .env.example):
- SIMTA_API_URL    : alamat dasar API, mis. https://simta.unj.ac.id/api/etl
- SIMTA_API_TOKEN  : token lengkap dari `php artisan etl:token` (termasuk "33|")
- SIMTA_KODE_PRODI : kode prodi, pisahkan dengan koma bila lebih dari satu
- SIMTA_PER_PAGE   : opsional, baris per halaman (maks 1000)

Token wajib punya ability untuk prodi yang diambil, kalau tidak API membalas 403.
"""
import os
import time
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv("/opt/airflow/repo/.env")

# Retry otomatis untuk gangguan jaringan (connection refused/reset, timeout)
# dan respons 429/5xx. Backoff bertingkat: 2, 4, 8, 16, 32 detik.
_RETRY = Retry(
    total=5,
    connect=5,
    read=3,
    status=5,
    backoff_factor=2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET"}),
    respect_retry_after_header=True,
    raise_on_status=False,
)


def _wajib(key: str) -> str:
    value = (os.getenv(key) or "").strip()
    if not value:
        raise ValueError(f"{key} belum diisi di /opt/airflow/repo/.env.")
    return value


def daftar_prodi() -> list:
    """Kode prodi dari SIMTA_KODE_PRODI, mis. "15136" atau "15136,15137"."""
    prodi = [k.strip() for k in _wajib("SIMTA_KODE_PRODI").split(",") if k.strip()]
    if not prodi:
        raise ValueError("SIMTA_KODE_PRODI tidak berisi kode prodi yang valid.")
    return prodi


def per_page(default: int = 100) -> int:
    """Baris per halaman dari SIMTA_PER_PAGE, dibatasi 1..1000."""
    try:
        nilai = int(os.getenv("SIMTA_PER_PAGE") or default)
    except ValueError:
        nilai = default
    return max(1, min(nilai, 1000))


class SimtaApi:
    """Pembungkus request SIMTA. Pakai `with SimtaApi() as api:`."""

    def __init__(self):
        self.base_url = _wajib("SIMTA_API_URL").rstrip("/")
        token = _wajib("SIMTA_API_TOKEN")
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=_RETRY)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        })

    def _request(self, url: str, params=None):
        resp = self.session.get(url, params=params, timeout=60)
        if not resp.ok:
            raise RuntimeError(f"GET {url} gagal: HTTP {resp.status_code} | {resp.text[:300]}")
        return resp.json()

    def iter_pages(self, path: str, params=None, jeda: float = 1.0):
        """Ambil semua halaman: mulai dari path, ikuti links.next sampai None.

        `path` relatif terhadap SIMTA_API_URL, mis. "prodi/15136/mahasiswa".
        Yield tiap elemen `data`. Aman untuk data besar (cursor pagination).
        `jeda` = detik istirahat antarhalaman supaya server tidak terbebani.
        """
        params = dict(params or {})
        # Halaman pertama pakai base_url + path + params.
        body = self._request(f"{self.base_url}/{path.lstrip('/')}", params=params)
        halaman = 0
        while True:
            halaman += 1
            data = body.get("data")
            if not isinstance(data, list):
                raise ValueError("Format respons tidak sesuai. Diharapkan `data` berupa array.")
            yield from data

            # links.next sudah berisi URL lengkap dengan cursor; None = habis.
            next_url = (body.get("links") or {}).get("next")
            if not next_url:
                break
            if halaman > 10000:  # sabuk pengaman dari loop tak berujung
                raise RuntimeError("Paginasi SIMTA melebihi 10000 halaman, dihentikan.")
            if jeda:
                time.sleep(jeda)
            body = self._request(_pertahankan_params(next_url, params))

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _pertahankan_params(url: str, params: dict) -> str:
    """Tambahkan params (mis. per_page) ke URL next kalau belum ada.

    Jaga-jaga bila links.next tidak membawa query string lama, sehingga
    halaman berikutnya tidak kembali ke per_page default.
    """
    if not params:
        return url
    bagian = urlparse(url)
    query = parse_qs(bagian.query, keep_blank_values=True)
    for key, value in params.items():
        query.setdefault(key, [str(value)])
    return urlunparse(bagian._replace(query=urlencode(query, doseq=True)))

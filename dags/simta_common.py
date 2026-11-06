"""Akses ke SIMTA API (Laravel). Token Bearer statis, paginasi cursor.

Konfigurasi diambil dari Airflow Connection `simta_api`:
- Password : token ETL (hasil `php artisan etl:token`), ability etl:read + prodi:*
- Extra    : {"base_url": "https://simta.unj.ac.id"}

Token wajib punya ability untuk prodi yang diambil, kalau tidak API membalas 403.
"""
import requests

try:
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 3.0.x
    from airflow.hooks.base import BaseHook

SIMTA_CONNECTION_ID = "simta_api"


class SimtaApi:
    """Pembungkus request SIMTA. Pakai `with SimtaApi() as api:`."""

    def __init__(self):
        conn = BaseHook.get_connection(SIMTA_CONNECTION_ID)
        if not conn.password:
            raise ValueError("Token SIMTA kosong. Isi di field Password connection simta_api.")
        self.base_url = conn.extra_dejson["base_url"].rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {conn.password}",
        })

    def _request(self, url: str, params=None):
        resp = self.session.get(url, params=params, timeout=60)
        if not resp.ok:
            raise RuntimeError(f"GET {url} gagal: HTTP {resp.status_code} | {resp.text[:300]}")
        return resp.json()

    def iter_pages(self, path: str, params=None):
        """Ambil semua halaman: mulai dari path, ikuti links.next sampai None.

        Yield tiap elemen `data`. Aman untuk data besar (cursor pagination).
        """
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
            body = self._request(next_url)

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

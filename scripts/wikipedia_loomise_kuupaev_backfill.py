"""
Täidab staging.wikipedia_raw veeru loomise_kuupaev artiklite loomiskuupäevaga.

Käivitus:
    DB_HOST=localhost DB_PORT=55432 python3 wikipedia_loomise_kuupaev_backfill.py
"""

import os
import time
from urllib.parse import unquote

import psycopg2
import requests

API_URL = "https://et.wikipedia.org/w/api.php"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
HTTP_TIMEOUT = 30
SCRAPE_DELAY_SEC = 1.0


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "analytics-db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "EKI"),
        password=os.environ.get("DB_PASSWORD", "ekipostgres"),
        dbname=os.environ.get("DB_NAME", "eki_postgres"),
    )


def _tomba_loomise_kuupaev(session: requests.Session, allikas_url: str) -> str | None:
    """Tõmbab artikli esimese revisjoni kuupäeva (= loomiskuupäev)."""
    # Eralda pageid URL-ist
    if "curid=" in allikas_url:
        pageid = allikas_url.split("curid=")[1].split("&")[0]
        params = {"pageids": pageid}
    elif "/wiki/" in allikas_url:
        pealkiri = unquote(allikas_url.split("/wiki/")[1])
        params = {"titles": pealkiri}
    else:
        return None

    resp = session.get(
        API_URL,
        params={
            "action": "query",
            "prop": "revisions",
            "rvlimit": 1,
            "rvdir": "newer",
            "rvprop": "timestamp",
            "format": "json",
            **params,
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    for page in pages.values():
        revisions = page.get("revisions") or []
        if revisions:
            return revisions[0].get("timestamp")
    return None


def main():
    conn = get_connection()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    with conn.cursor() as cur:
        cur.execute("""
            SELECT doc_id, allikas_url
            FROM staging.wikipedia_raw
            WHERE loomise_kuupaev IS NULL
              AND tekst IS NOT NULL
        """)
        read = cur.fetchall()

    print(f"Artikleid ilma loomiskuupäevata: {len(read)}")

    uuendatud = 0
    for i, (doc_id, allikas_url) in enumerate(read):
        loomise_kuupaev = _tomba_loomise_kuupaev(session, allikas_url)
        if loomise_kuupaev:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE staging.wikipedia_raw SET loomise_kuupaev = %s WHERE doc_id = %s",
                    (loomise_kuupaev, doc_id),
                )
            conn.commit()
            uuendatud += 1

        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(read)} — {uuendatud} uuendatud")

        time.sleep(SCRAPE_DELAY_SEC)

    conn.close()
    print(f"\nKokku uuendatud: {uuendatud}/{len(read)}")


if __name__ == "__main__":
    main()

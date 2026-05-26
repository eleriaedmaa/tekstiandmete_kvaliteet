"""
Ühekordne migratsioon — laeb seeds/teadaolevad_dokumendid.csv URLid staging tabelitesse.

Käivita üks kord pärast `dbt seed`:
    docker compose exec airflow-apiserver python /opt/airflow/scripts/migrate_seed_to_staging.py

- riigikogu, rahvaalgatus: lisatakse URL-stub (tekst puudub, blokeerib uuesti kogumise)
- wikipedia/wiki: kontrollitakse muutmiskuupäeva, kui >= 01.01.2026 tõmmatakse tekst

Toetab kahte Wikipedia URL-formaati:
    https://et.wikipedia.org/wiki/Pealkiri
    https://et.wikipedia.org/?curid=12345
"""

import hashlib
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
import requests

WIKIPEDIA_API_URL = "https://et.wikipedia.org/w/api.php"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
SCRAPE_DELAY_SEC = 2.0
WIKI_PIIRKUUPAEV = datetime(2026, 1, 1, tzinfo=timezone.utc)

STUB_TABELID = {
    "riigikogu":    "staging.riigikogu_raw",
    "rahvaalgatus": "staging.rahvaalgatus_raw",
}


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "analytics-db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "EKI"),
        password=os.environ.get("DB_PASSWORD", "ekipostgres"),
        dbname=os.environ.get("DB_NAME", "eki_postgres"),
    )


def _wiki_api_params(url: str) -> dict | None:
    """Tagastab Wikipedia API otsinguparameetrid URL-i formaadi põhjal."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "curid" in qs:
        return {"pageids": qs["curid"][0]}
    if "/wiki/" in parsed.path:
        pealkiri = unquote(parsed.path.split("/wiki/", 1)[1])
        if pealkiri:
            return {"titles": pealkiri}
    return None


def _viimane_muutmiskuupaev(session: requests.Session, params: dict) -> datetime | None:
    resp = session.get(
        WIKIPEDIA_API_URL,
        params={"action": "query", "prop": "revisions", "rvprop": "timestamp",
                "redirects": 1, "format": "json", **params},
        timeout=30,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()), {})
    revisions = page.get("revisions", [])
    if not revisions:
        return None
    try:
        return datetime.fromisoformat(revisions[0]["timestamp"].replace("Z", "+00:00"))
    except (KeyError, ValueError):
        return None


def _tomba_wikipedia_artikkel(session: requests.Session, params: dict) -> tuple[str | None, str | None]:
    resp = session.get(
        WIKIPEDIA_API_URL,
        params={"action": "query", "prop": "extracts", "explaintext": 1,
                "redirects": 1, "format": "json", **params},
        timeout=30,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()), {})
    return page.get("title"), page.get("extract")


def _lisa_stub(cur, run_id, tabel: str, url: str, now: datetime) -> bool:
    doc_id = "seed_" + hashlib.md5(url.encode()).hexdigest()
    cur.execute(
        f"INSERT INTO {tabel} (run_id, doc_id, allikas_url, laetud_kell) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (run_id, doc_id, url, now),
    )
    return bool(cur.rowcount)


def _lisa_wikipedia(cur, session: requests.Session, run_id, url: str, now: datetime) -> bool:
    params = _wiki_api_params(url)
    if not params:
        print(f"  [VAHELE] Tundmatu URL-formaat: {url}")
        return False

    print(f"  [WIKI] Kontrollin: {url}")
    muudetud = _viimane_muutmiskuupaev(session, params)
    time.sleep(SCRAPE_DELAY_SEC)

    if muudetud is None or muudetud < WIKI_PIIRKUUPAEV:
        print(f"  [VAHELE] Vana ({muudetud}): {url}")
        return False

    print(f"  [TÕMBAN] Muudetud {muudetud}: {url}")
    pealkiri, tekst = _tomba_wikipedia_artikkel(session, params)
    time.sleep(SCRAPE_DELAY_SEC)

    if not tekst:
        print(f"  [VAHELE] Tekst puudub: {url}")
        return False

    doc_id = "seed_" + hashlib.md5(url.encode()).hexdigest()
    cur.execute(
        """
        INSERT INTO staging.wikipedia_raw (run_id, doc_id, pealkiri, tekst, allikas_url, laetud_kell)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (run_id, doc_id, pealkiri, tekst, url, now),
    )
    return bool(cur.rowcount)


def main():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('SELECT allikas, "URL" FROM marts.teadaolevad_dokumendid WHERE "URL" IS NOT NULL')
    kirjed = cur.fetchall()

    if not kirjed:
        print("Seed-tabel on tühi, midagi pole teha.")
        sys.exit(0)

    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO staging.pipeline_runs (run_id, fetched_at, source_name, status, message)
        VALUES (gen_random_uuid(), %s, 'migration', 'success', 'Ajalooliste URL-ide ühekordne import seedist')
        RETURNING run_id
        """,
        (now,),
    )
    run_id = cur.fetchone()[0]
    conn.commit()

    # Lae juba töödeldud URLid mällu
    cur.execute(
        """
        SELECT allikas_url FROM staging.wikipedia_raw WHERE doc_id LIKE 'seed_%'
        UNION
        SELECT allikas_url FROM staging.riigikogu_raw WHERE doc_id LIKE 'seed_%'
        UNION
        SELECT allikas_url FROM staging.rahvaalgatus_raw WHERE doc_id LIKE 'seed_%'
        """
    )
    juba_tooteldud = {row[0] for row in cur.fetchall()}
    print(f"Juba töödeldud: {len(juba_tooteldud)} URL-i")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    lisatud = 0
    vahele = 0

    kokku = len(kirjed)
    for i, (allikas, url) in enumerate(kirjed, 1):
        if i % 1000 == 0:
            print(f"  {i}/{kokku} — lisatud: {lisatud}, vahele: {vahele}")
        if url in juba_tooteldud:
            vahele += 1
            continue
        try:
            if allikas in STUB_TABELID:
                ok = _lisa_stub(cur, run_id, STUB_TABELID[allikas], url, now)
            elif allikas in ("wikipedia", "wiki"):
                ok = _lisa_wikipedia(cur, session, run_id, url, now)
            else:
                print(f"Tundmatu allikas '{allikas}': {url}")
                continue

            conn.commit()
            if ok:
                lisatud += 1
            else:
                vahele += 1

        except Exception as e:
            conn.rollback()
            print(f"Viga ({allikas} {url}): {e}")

    cur.close()
    conn.close()
    print(f"Valmis. Lisatud: {lisatud}, juba olemas/vahele: {vahele}")


if __name__ == "__main__":
    main()

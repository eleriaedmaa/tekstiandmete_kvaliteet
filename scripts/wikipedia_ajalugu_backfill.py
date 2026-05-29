"""
Laadib staging.wikipedia_raw kõik uued Eesti Wikipedia artiklid ajavahemikust
2016-01-01 kuni 2026-05-19 (enne DAG-i käivitust), välja arvatud artiklid,
mis on juba seedis (marts.teadaolevad_dokumendid).

Skript töötab kuude kaupa ja saab katkestada ning jätkata —
olemasolevad artiklid jäetakse vahele (ON CONFLICT DO NOTHING + eelkontroll).

Käivitus:
    DB_HOST=localhost DB_PORT=55432 python3 wikipedia_ajalugu_backfill.py

Ajavahemiku muutmiseks:
    ALGUS=2020-01-01 LOPP=2023-12-31 DB_HOST=localhost DB_PORT=55432 python3 wikipedia_ajalugu_backfill.py
"""

import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

import psycopg2
import requests

API_URL = "https://et.wikipedia.org/w/api.php"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
HTTP_TIMEOUT = 30
SCRAPE_DELAY_SEC = 1.0

ALGUS = os.environ.get("ALGUS", "2016-01-01")
LOPP  = os.environ.get("LOPP",  "2026-05-19")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "analytics-db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "EKI"),
        password=os.environ.get("DB_PASSWORD", "ekipostgres"),
        dbname=os.environ.get("DB_NAME", "eki_postgres"),
    )


def laadi_seed_pealkirjad(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT pealkiri FROM marts.teadaolevad_dokumendid WHERE allikas = 'wiki' AND pealkiri IS NOT NULL AND pealkiri != ''")
        # normaliseeri: asenda alakriipsud tühikutega, et kattuda logevents pealkirjadega
        return {row[0].replace("_", " ") for row in cur.fetchall()}


def laadi_olemasolevad_doc_id(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT doc_id FROM staging.wikipedia_raw")
        return {row[0] for row in cur.fetchall()}


def otsi_uued_artiklid_kuus(session: requests.Session, algus: str, lopp: str) -> list[dict]:
    """Tagastab kõik uued artiklid antud ajavahemikus logevents API kaudu."""
    tulemused = []
    params = {
        "action": "query",
        "list": "logevents",
        "letype": "create",
        "lenamespace": 0,
        "lelimit": 500,
        "lestart": lopp,
        "leend": algus,
        "leprop": "title|timestamp|ids",
        "format": "json",
    }
    while True:
        resp = session.get(API_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        tulemused.extend(data.get("query", {}).get("logevents", []))
        if "continue" not in data:
            break
        params.update(data["continue"])
        time.sleep(0.5)
    return tulemused


def tomba_artikkel(session: requests.Session, pageid: int) -> tuple[str | None, str | None]:
    """Tagastab (pealkiri, tekst) extracts API kaudu."""
    resp = session.get(
        API_URL,
        params={
            "action": "query",
            "pageids": pageid,
            "prop": "extracts",
            "explaintext": 1,
            "format": "json",
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    page = pages.get(str(pageid), {})
    return page.get("title"), page.get("extract")


def kuu_vahemikud(algus: str, lopp: str) -> list[tuple[str, str]]:
    """Jagab ajavahemiku kuude kaupa (uuemast vanemani)."""
    algus_dt = datetime.strptime(algus, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    lopp_dt  = datetime.strptime(lopp,  "%Y-%m-%d").replace(tzinfo=timezone.utc)
    vahemikud = []
    praegu = lopp_dt
    while praegu > algus_dt:
        kuu_algus = (praegu.replace(day=1))
        if kuu_algus < algus_dt:
            kuu_algus = algus_dt
        vahemikud.append((
            kuu_algus.strftime("%Y-%m-%dT00:00:00Z"),
            praegu.strftime("%Y-%m-%dT23:59:59Z"),
        ))
        praegu = kuu_algus - timedelta(days=1)
    return vahemikud


def main():
    conn = get_connection()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print("Laadin seed-pealkirjad...")
    seed_pealkirjad = laadi_seed_pealkirjad(conn)
    print(f"  {len(seed_pealkirjad)} pealkirja seedis")

    print("Laadin olemasolevad doc_id-d...")
    olemasolevad = laadi_olemasolevad_doc_id(conn)
    print(f"  {len(olemasolevad)} artiklit juba andmebaasis")

    vahemikud = kuu_vahemikud(ALGUS, LOPP)
    print(f"Töötlen {len(vahemikud)} kuud ({ALGUS} kuni {LOPP})\n")

    sql = """
        INSERT INTO staging.wikipedia_raw
            (run_id, doc_id, pealkiri, tekst, revid, avaldatud, loomise_kuupaev, allikas_url, laetud_kell)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    kokku_lisatud = 0
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO staging.pipeline_runs (run_id, fetched_at, source_name, status) VALUES (%s, %s, %s, %s)",
            (run_id, datetime.now(timezone.utc), "wikipedia", "running"),
        )
    conn.commit()

    for i, (kuu_algus, kuu_lopp) in enumerate(vahemikud):
        kuu_label = kuu_algus[:7]
        artiklid = otsi_uued_artiklid_kuus(session, kuu_algus, kuu_lopp)

        # filtreeri seed ja juba andmebaasis olevad
        uued = []
        for a in artiklid:
            pageid = a.get("pagid") or a.get("pageid")
            doc_id = f"backfill_{pageid}" if pageid else f"backfill_{a.get('title')}"
            if a.get("title") not in seed_pealkirjad and doc_id not in olemasolevad:
                uued.append(a)

        print(f"[{i+1}/{len(vahemikud)}] {kuu_label}: {len(artiklid)} artiklit logevents-ist, {len(uued)} uut pärast filtrit")
        sys.stdout.flush()

        for artikkel in uued:
            pageid   = artikkel.get("pagid") or artikkel.get("pageid")
            pealkiri_log = artikkel.get("title")
            loomise_kuupaev = artikkel.get("timestamp")
            doc_id   = f"backfill_{pageid}" if pageid else f"backfill_{pealkiri_log}"

            if doc_id in olemasolevad:
                continue

            if pageid:
                pealkiri, tekst = tomba_artikkel(session, pageid)
                time.sleep(SCRAPE_DELAY_SEC)
            else:
                pealkiri, tekst = pealkiri_log, None

            if not tekst:
                continue

            if pealkiri and pealkiri in seed_pealkirjad:
                continue

            allikas_url = f"https://et.wikipedia.org/?curid={pageid}" if pageid else f"https://et.wikipedia.org/wiki/{pealkiri_log}"
            now = datetime.now(timezone.utc)

            with conn.cursor() as cur:
                cur.execute(sql, (
                    run_id,
                    doc_id,
                    pealkiri or pealkiri_log,
                    tekst,
                    None,
                    loomise_kuupaev,
                    loomise_kuupaev,
                    allikas_url,
                    now,
                ))
            conn.commit()
            olemasolevad.add(doc_id)
            kokku_lisatud += 1

        print(f"  → kokku lisatud seni: {kokku_lisatud}")
        sys.stdout.flush()

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE staging.pipeline_runs SET status = %s, docs_added = %s WHERE run_id = %s",
            ("success", kokku_lisatud, run_id),
        )
    conn.commit()
    conn.close()
    print(f"\nValmis. Lisatud kokku: {kokku_lisatud} artiklit.")


if __name__ == "__main__":
    main()

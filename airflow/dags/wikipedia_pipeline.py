"""
Eesti Wikipedia sissevõtt — MediaWiki action API.

Kasutab list=recentchanges, et leida viimase päeva muudetud artiklid,
seejärel prop=extracts, et tõmmata iga artikli puhastatud tekst.

API dokumentatsioon: https://et.wikipedia.org/w/api.php
"""

import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://et.wikipedia.org/w/api.php"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
HTTP_TIMEOUT = 30
SCRAPE_DELAY_SEC = 1.0                     # viisakuspaus iga päringu järel (väldib 429 vigu)
SOURCE_NAME = "wikipedia"

def _registreeri_kaivitus(hook: PostgresHook, run_id: str) -> None:
    hook.run(
        """
        INSERT INTO staging.pipeline_runs (run_id, fetched_at, source_name, status)
        VALUES (%s, %s, %s, 'running')
        """,
        parameters=(run_id, datetime.now(timezone.utc), SOURCE_NAME),
    )


def _markeri_loppu(hook: PostgresHook, run_id: str, status: str, docs_added: int, message: str | None = None) -> None:
    hook.run(
        """
        UPDATE staging.pipeline_runs
        SET status = %s, docs_added = %s, message = %s
        WHERE run_id = %s
        """,
        parameters=(status, docs_added, message, run_id),
    )


def _otsi_uued_artiklid(session: requests.Session, execution_date: datetime) -> list[dict]:
    """Tagastab execution_date päeva muudetud artiklite (pageid, revid, title) nimekirja (kõik lehed)."""
    alates = (execution_date - timedelta(days=1)).isoformat()
    kuni = execution_date.isoformat()
    tulemused: list[dict] = []
    params: dict = {
        "action": "query",
        "list": "recentchanges",
        "rcstart": alates,
        "rcend": kuni,
        "rcdir": "newer",
        "rcnamespace": 0,
        "rctype": "new",
        "rcprop": "title|ids|timestamp|type|sizes",
        "rclimit": 500,
        "format": "json",
    }
    while True:
        resp = session.get(API_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        tulemused.extend(data.get("query", {}).get("recentchanges", []))
        if "continue" not in data:
            break
        params.update(data["continue"])
    return tulemused


def _tomba_artikli_tekst(session: requests.Session, pageid: int) -> tuple[str | None, str | None, str | None]:
    """Tagastab artikli (pealkiri, tekst, loomise_kuupaev) extracts+revisions API-st."""
    resp = session.get(
        API_URL,
        params={
            "action": "query",
            "pageids": pageid,
            "prop": "extracts|revisions",
            "explaintext": 1,                    # ilma HTML-margukestadeta
            "rvlimit": 1,                        # ainult esimene revisjon
            "rvdir": "newer",                    # vanaimast uuemani → esimene = loomiskuupäev
            "rvprop": "timestamp",
            "format": "json",
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    page = pages.get(str(pageid), {})
    revisions = page.get("revisions") or []
    loomise_kuupaev = revisions[0].get("timestamp") if revisions else None
    return page.get("title"), page.get("extract"), loomise_kuupaev


def lae_wikipedia(**context):
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    _registreeri_kaivitus(hook, run_id)

    docs_added = 0
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        muudatused = _otsi_uued_artiklid(session, context["data_interval_end"])
        now = datetime.now(timezone.utc)
        sql = """
            INSERT INTO staging.wikipedia_raw
                (run_id, doc_id, pealkiri, tekst, revid, avaldatud, loomise_kuupaev, allikas_url, laetud_kell)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        # Lae seedis olevad pealkirjad — neid artikleid ei lisata
        # URL-ist eraldame pealkirja ja dekodeerime %XX kodeeringu (nt %C3%A4 → ä)
        seed_pealkirjad: set[str] = set(
            unquote(row[0].split("/wiki/")[-1]).replace("_", " ")
            for row in hook.get_records(
                "SELECT \"URL\" FROM marts.teadaolevad_dokumendid WHERE allikas = 'wiki'"
            )
            if "/wiki/" in row[0]
        )

        # Dedupliseeri pageid kaupa (recentchanges võib sama artikli mitu korda tagastada)
        kasitletud_pageid: set[int] = set()
        for m in muudatused:
            pageid = m.get("pageid")
            revid = m.get("revid")
            if not pageid or pageid in kasitletud_pageid:
                continue
            kasitletud_pageid.add(pageid)

            pealkiri, tekst, loomise_kuupaev = _tomba_artikli_tekst(session, pageid)
            time.sleep(SCRAPE_DELAY_SEC)
            if pealkiri and pealkiri in seed_pealkirjad:
                continue
            doc_id = f"{pageid}_{revid}" if revid else str(pageid)
            url = f"https://et.wikipedia.org/?curid={pageid}"

            hook.run(sql, parameters=(
                run_id,
                doc_id,
                pealkiri,
                tekst,
                revid,
                m.get("timestamp"),
                loomise_kuupaev,
                url,
                now,
            ))
            docs_added += 1

        _markeri_loppu(hook, run_id, "success", docs_added, f"Toodi {docs_added} artiklit")

    except Exception as exc:
        _markeri_loppu(hook, run_id, "failed", docs_added, str(exc))
        raise


with DAG(
    dag_id="wikipedia_pipeline",
    description="Laeb Wikipedia uued/muudetud artiklid staging.wikipedia_raw tabelisse",
    schedule="@daily",
    start_date=datetime(2026, 5, 20),
    catchup=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["wikipedia", "tekstiandmete-kvaliteet", "ingest"],
) as dag:

    PythonOperator(
        task_id="lae_wikipedia",
        python_callable=lae_wikipedia,
    )

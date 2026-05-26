"""
Rahvaalgatus.ee sissevõtt — algatuste metaandmed API-st, täistekst scraperist.

API dokumentatsioon: https://app.swaggerhub.com/apis-docs/rahvaalgatus/rahvaalgatus/1
Põhiandmevoog: algatuste metaandmed (API) ja täistekst (scraper)
API: ainult initiatives: id, for, title, phase, signingEndsAt, signatureCount, signatureThreshold
API päringust jäid välja statistics ja initiative events. Initiative events ei ole API kaudu filtreeritavad

Samm 1: API tagastab algatuste nimekirja lehekülgede kaupa (pagination, limit=50).
        NB! Vajab spetsiifilist Accept-päist:
        Accept: application/vnd.rahvaalgatus.initiative+json; v=1
Samm 2: iga algatuse kohta tõmmatakse HTML-leht ja puhastatakse navigatsioonimüra
        BeautifulSoup-iga (robots.txt: Disallow: — kõik lubatud).
"""

import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup

API_LIST_URL = "https://rahvaalgatus.ee/initiatives"
SITE_BASE = "https://rahvaalgatus.ee"
ACCEPT_HEADER = "application/vnd.rahvaalgatus.initiative+json; v=1"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
HTTP_TIMEOUT = 30
SCRAPE_DELAY_SEC = 1.0                     # viisakuspaus iga päringu järel
PAGINATION_LIMIT = 2000                    # maksimaalne kirjete arv ühe API päringu kohta
SOURCE_NAME = "rahvaalgatus"


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


def _tomba_algatuste_nimekiri(session: requests.Session) -> list:
    """Tõmbab kõik algatused API-st lehekülgede kaupa.

    API toetab offset-põhist paginatsiooni. Duplikaadid välditakse
    seen_ids set-iga juhuks, kui API tagastab kattuvaid kirjeid.
    """
    all_initiatives = []
    seen_ids = set()  # duplikaadid
    offset = 0

    while True:
        resp = session.get(
            API_LIST_URL,
            headers={"Accept": ACCEPT_HEADER},
            params={
                "limit": PAGINATION_LIMIT,
                "offset": offset,
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        batch = resp.json()
        if isinstance(batch, dict):
            batch = batch.get("initiatives", batch.get("data", []))

        # Peatu, kui partii on tühi
        if not batch:
            break

        # Peatu, kui leiti ID, mis juba eksisteerib
        new_items = [item for item in batch if str(item.get("id", "")) not in seen_ids]
        if not new_items:
            break

        for item in new_items:
            seen_ids.add(str(item["id"]))
            all_initiatives.append(item)

        if len(batch) < PAGINATION_LIMIT:
            break

        offset += PAGINATION_LIMIT
        time.sleep(0.5)

    return all_initiatives


def _tomba_taistekst(session: requests.Session, url: str) -> str | None:
    """Tõmbab algatuse HTML-i ja eraldab sisuosa, filtreerides välja navigatsioonimüra."""
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException:
        return None

    resp.encoding = resp.apparent_encoding or "utf-8"
    soup = BeautifulSoup(resp.text, "lxml")

    # Eemalda jalus
    for footer in soup.find_all("footer"):
        footer.decompose()

    # Eemalda jälgijatega seotud <p>
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if "Algatuse jälgijaid on" in text and "kõiki algatusi korraga jälgib" in text:
            p.decompose()

    # Väljavõte kõigist lõikudest ja pealkirjadest
    body_paragraphs = []

    for tag in soup.find_all(["p", "li", "h2", "h3"]):
        text = tag.get_text(strip=True)

        # Jäta vahele navigatsiooni, allkirjastamise jms seotud info
        if any(skip in text for skip in [
            "ALLKIRJASTA", "Allkirjasta", "Logi sisse", "Esileht",
            "allkirja puudu", "Arvesse läheb", "Tahad aidata",
            "Jälgi algatust", "Kommenteeri",
            "Telefoninumber", "Isikukood", "Smart-ID", "Mobile ID",
            "A. Weizenbergi", "info@rahvaalgatus", "fb.me/rahvaalgatus",
        ]):
            continue

        if text and len(text) > 20:
            body_paragraphs.append(text)

    if not body_paragraphs:
        return None

    return "\n\n".join(body_paragraphs)


def lae_rahvaalgatus(**context):
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    _registreeri_kaivitus(hook, run_id)

    docs_added = 0
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        # 1. API: algatuste nimekiri kõikidelt lehekülgedelt
        algatused = _tomba_algatuste_nimekiri(session)

        now = datetime.now(timezone.utc)
        sql = """
            INSERT INTO staging.rahvaalgatus_raw
                (run_id, doc_id, pealkiri, tekst, staatus, avaldatud, allikas_url, laetud_kell)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        # 2. Scraper: iga algatuse täistekst
        for a in algatused:
            doc_id = str(a.get("id") or "")
            if not doc_id:
                continue
            url = f"{SITE_BASE}/initiatives/{doc_id}"
            tekst = _tomba_taistekst(session, url)
            time.sleep(SCRAPE_DELAY_SEC)

            hook.run(sql, parameters=(
                run_id,
                doc_id,
                a.get("title"),
                tekst,
                a.get("phase"),
                a.get("signingEndsAt"),
                url,
                now,
            ))
            docs_added += 1

        _markeri_loppu(hook, run_id, "success", docs_added, f"Toodi {docs_added} algatust")

    except Exception as exc:
        _markeri_loppu(hook, run_id, "failed", docs_added, str(exc))
        raise


with DAG(
    dag_id="rahvaalgatus_pipeline",
    description="Laeb Rahvaalgatuse algatuste metaandmed + täisteksti staging.rahvaalgatus_raw tabelisse",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    tags=["rahvaalgatus", "tekstiandmete-kvaliteet", "ingest"],
) as dag:

    PythonOperator(
        task_id="lae_rahvaalgatus",
        python_callable=lae_rahvaalgatus,
    )

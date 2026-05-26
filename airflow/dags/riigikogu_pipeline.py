"""
Riigikogu sissevõtt — täiskogu istungite stenogrammid.

Kasutab /api/steno/verbatims endpoint-i, mis tagastab istungite stenogrammide
täisteksti (kõnelejate kaupa). Iga istung = üks dokument; teksti kogumiseks
ühendatakse kõik SPEECH tüüpi sündmused üheks tekstiks.

API: https://api.riigikogu.ee/v3/api-docs
"""

import uuid
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://api.riigikogu.ee/api/steno/verbatims"
USER_AGENT = "tekstiandmete-kvaliteet-praktikum/1.0 (ELTL; kontakt: praktikum@eki.ee)"
HTTP_TIMEOUT = 60
SOURCE_NAME = "riigikogu"
LOOKBACK_PAEVI = 7


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


def _doc_id_lingist(link: str) -> str:
    """Stabiilne doc_id stenogrammi lingist (nt 202605201400)."""
    if not link:
        return str(uuid.uuid4())
    return link.rstrip("/").rsplit("/", 1)[-1]


def _korva_stenogramm(verbatim: dict) -> str:
    """Korjab kokku kõik SPEECH tüüpi sündmuste tekstid üheks dokumendiks."""
    tekstid: list[str] = []
    for agenda in verbatim.get("agendaItems", []) or []:
        for sundmus in agenda.get("events", []) or []:
            if sundmus.get("type") == "SPEECH" and sundmus.get("text"):
                speaker = sundmus.get("speaker") or ""
                tekst = sundmus["text"].strip()
                if speaker:
                    tekstid.append(f"{speaker}: {tekst}")
                else:
                    tekstid.append(tekst)
    return "\n\n".join(tekstid)


def lae_riigikogu(**context):
    hook = PostgresHook(postgres_conn_id="analytics_db")
    run_id = str(uuid.uuid4())
    _registreeri_kaivitus(hook, run_id)

    docs_added = 0
    try:
        lopp = datetime.now(timezone.utc).date()
        algus = lopp - timedelta(days=LOOKBACK_PAEVI)

        resp = requests.get(
            API_URL,
            params={"startDate": algus.isoformat(), "endDate": lopp.isoformat()},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        verbatims = resp.json()
        if not isinstance(verbatims, list):
            verbatims = verbatims.get("content") or verbatims.get("_embedded", {}).get("content") or []

        now = datetime.now(timezone.utc)
        sql = """
            INSERT INTO staging.riigikogu_raw
                (run_id, doc_id, pealkiri, tekst, dok_tyyp, avaldatud, allikas_url, laetud_kell)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        for v in verbatims:
            link = v.get("link") or ""
            doc_id = _doc_id_lingist(link)
            tekst = _korva_stenogramm(v)
            if not tekst:
                continue
            hook.run(sql, parameters=(
                run_id,
                doc_id,
                v.get("title"),
                tekst,
                "stenogramm",
                v.get("date"),
                link,
                now,
            ))
            docs_added += 1

        _markeri_loppu(hook, run_id, "success", docs_added, f"Toodi {docs_added} stenogrammi")

    except Exception as exc:
        _markeri_loppu(hook, run_id, "failed", docs_added, str(exc))
        raise


with DAG(
    dag_id="riigikogu_pipeline",
    description="Laeb Riigikogu täiskogu istungite stenogrammid staging.riigikogu_raw tabelisse",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    tags=["riigikogu", "tekstiandmete-kvaliteet", "ingest"],
) as dag:

    PythonOperator(
        task_id="lae_riigikogu",
        python_callable=lae_riigikogu,
    )

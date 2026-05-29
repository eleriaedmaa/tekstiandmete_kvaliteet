"""Streamlit näidikulaud tekstiandmete kvaliteedi mõõdikuteks."""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


st.set_page_config(
    page_title="Tekstiandmete kvaliteet",
    layout="wide",
)

ALLIKA_NIMED = {
    "riigikogu":    "Riigikogu",
    "rahvaalgatus": "Rahvaalgatus.ee",
    "wikipedia":    "Eesti Wikipedia",
}
ALLIKA_VARVID = {
    "riigikogu":    "#1f77b4",
    "rahvaalgatus": "#2ca02c",
    "wikipedia":    "#ff7f0e",
}
PUUDUSE_NIMED = {
    "liiga_lyhike":   "Liiga lühike",
    "vale_keel":      "Vale keel",
    "duplikaat":      "Duplikaat",
    "puudus_puudub":  "Puudus puudub",
}


def get_int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "analytics-db"),
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ.get("DB_USER", "praktikum"),
        password=os.environ.get("DB_PASSWORD", "praktikum"),
        dbname=os.environ.get("DB_NAME", "praktikum"),
    )


def load_df(query: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


auto_refresh = get_int_env("DASHBOARD_AUTOREFRESH_SECONDS", 60)
if auto_refresh > 0 and st_autorefresh is not None:
    st_autorefresh(interval=auto_refresh * 1000, key="autorefresh")

if st.sidebar.button("Värskenda vaade"):
    st.rerun()

st.title("Tekstiandmete kvaliteet — kogumismõõdikud")
st.caption("Andmeallikad: Riigikogu, Rahvaalgatus.ee, Eesti Wikipedia")

try:
    quality = load_df(
        """
        SELECT
            allikas,
            laetud_kuupaev,
            dokumente_kokku,
            sonu_kokku,
            sonu_kasutatav,
            kasutatavuse_pct,
            liiga_lyhike_pct,
            vale_keel_pct,
            duplikaat_pct,
            peamine_kvaliteedipuudus
        FROM marts.mart_source_quality
        ORDER BY laetud_kuupaev, allikas
        """
    )
    docs = load_df(
        """
        SELECT allikas, laetud_kuupaev, word_count, kasutatav, peamine_pohjus
        FROM marts.fct_documents
        """
    )
    katvus = load_df(
        """
        SELECT allikas, avaldatud_kuupaev, dokumente_kokku, sonu_kokku, sonu_kasutatav
        FROM marts.mart_avaldamise_katvus
        WHERE avaldatud_kuupaev >= '2016-01-01'
        ORDER BY avaldatud_kuupaev, allikas
        """
    )
    runs = load_df(
        """
        SELECT run_id::text AS run_id, source_name, fetched_at, status, docs_added, message
        FROM staging.pipeline_runs
        ORDER BY fetched_at DESC
        LIMIT 20
        """
    )
except Exception as exc:
    st.error(
        f"Ei õnnestunud lugeda andmebaasi: {exc}\n\n"
        "Kontrolli, et Airflow DAG-id on käivitunud ja dbt run + dbt test on edukalt läbinud."
    )
    st.stop()

if quality.empty:
    st.warning(
        "Andmeid ei ole veel laaditud. Käivita Airflow UI-s järjest "
        "`riigikogu_pipeline`, `rahvaalgatus_pipeline`, `wikipedia_pipeline` "
        "ja seejärel `dbt_pipeline`."
    )
    st.stop()

quality["laetud_kuupaev"] = pd.to_datetime(quality["laetud_kuupaev"]).dt.normalize()
quality["allikas_nimi"] = quality["allikas"].map(ALLIKA_NIMED).fillna(quality["allikas"])
quality["laetud_kuupaev_str"] = quality["laetud_kuupaev"].dt.strftime("%d.%m.%Y")

# --- KPI kaardid ---
viimane_kpv = quality["laetud_kuupaev"].max()
viimase_paeva = quality[quality["laetud_kuupaev"] == viimane_kpv]

sonu_kokku_koik = int(quality["sonu_kokku"].sum())
sonu_kokku_viimane = int(viimase_paeva["sonu_kokku"].sum())
sonu_kasutatav_koik = int(quality["sonu_kasutatav"].sum())
kasutatavus_kaalutud = (
    quality["sonu_kasutatav"].sum() / quality["sonu_kokku"].sum() * 100
    if quality["sonu_kokku"].sum() > 0 else 0
)

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Sõnu kogutud kokku",
    f"{sonu_kokku_koik:,}".replace(",", " "),
)
k2.metric(
    "Sõnu lisandunud (viimane päev)",
    f"{sonu_kokku_viimane:,}".replace(",", " "),
)
k3.metric(
    "Kasutatavaid sõnu kokku",
    f"{sonu_kasutatav_koik:,}".replace(",", " "),
)
k4.metric(
    "Keskmine kasutatavus",
    f"{kasutatavus_kaalutud:.1f}%",
)

# --- Korpuse ajaline katvus ---
st.subheader("Uute andmete ajajoon — avaldamiskuupäeva järgi")

if not katvus.empty:
    katvus["avaldatud_kuupaev"] = pd.to_datetime(katvus["avaldatud_kuupaev"]).dt.normalize()
    katvus["allikas_nimi"] = katvus["allikas"].map(ALLIKA_NIMED).fillna(katvus["allikas"])
    katvus["kuu"] = katvus["avaldatud_kuupaev"].dt.to_period("M").astype(str)
    katvus_kuu = (
        katvus.groupby(["allikas_nimi", "kuu"], as_index=False)
        .agg(sonu_kasutatav=("sonu_kasutatav", "sum"), dokumente_kokku=("dokumente_kokku", "sum"))
    )
    katvus_diagramm = (
        alt.Chart(katvus_kuu)
        .mark_bar()
        .encode(
            x=alt.X("kuu:O", title="Kuu", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("dokumente_kokku:Q", title="Dokumente", stack="zero"),
            color=alt.Color(
                "allikas_nimi:N",
                title="Allikas",
                scale=alt.Scale(
                    domain=list(ALLIKA_NIMED.values()),
                    range=[ALLIKA_VARVID[k] for k in ALLIKA_NIMED.keys()],
                ),
            ),
            tooltip=[
                alt.Tooltip("kuu:O", title="Kuu"),
                alt.Tooltip("allikas_nimi:N", title="Allikas"),
                alt.Tooltip("dokumente_kokku:Q", title="Dokumente"),
                alt.Tooltip("sonu_kasutatav:Q", title="Kasutatavaid sõnu"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(katvus_diagramm, use_container_width=True)

# --- Mõõdik 1: sõnade lisandumine ajas ---
st.subheader("Mõõdik 1 — uute sõnade lisandumine ajas")

if not katvus.empty:
    moodik1 = katvus[katvus["avaldatud_kuupaev"] >= pd.Timestamp("2026-05-22")].copy()
    moodik1["avaldatud_kuupaev_str"] = moodik1["avaldatud_kuupaev"].dt.strftime("%d.%m.%Y")
    sonad_ajas = (
        alt.Chart(moodik1)
        .mark_bar()
        .encode(
            x=alt.X("avaldatud_kuupaev_str:O", title="Kuupäev",
                    sort=moodik1["avaldatud_kuupaev_str"].tolist(),
                    axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("sonu_kasutatav:Q", title="Kasutatavad sõnad", stack="zero"),
            color=alt.Color(
                "allikas_nimi:N",
                title="Allikas",
                scale=alt.Scale(
                    domain=list(ALLIKA_NIMED.values()),
                    range=[ALLIKA_VARVID[k] for k in ALLIKA_NIMED.keys()],
                ),
            ),
            tooltip=[
                alt.Tooltip("avaldatud_kuupaev_str:O", title="Kuupäev"),
                alt.Tooltip("allikas_nimi:N", title="Allikas"),
                alt.Tooltip("dokumente_kokku:Q", title="Dokumente"),
                alt.Tooltip("sonu_kasutatav:Q", title="Kasutatavaid sõnu"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(sonad_ajas, use_container_width=True)

# --- Mõõdik 2: kasutatavuse % ---
st.subheader("Mõõdik 2 — kasutatavuse % allika kohta")

kasutatavus = (
    alt.Chart(quality)
    .mark_bar()
    .encode(
        x=alt.X("laetud_kuupaev_str:O", title="Kuupäev", sort=quality["laetud_kuupaev_str"].tolist()),
        y=alt.Y("kasutatavuse_pct:Q", title="Kasutatavus %", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color(
            "allikas_nimi:N",
            title="Allikas",
            scale=alt.Scale(
                domain=list(ALLIKA_NIMED.values()),
                range=[ALLIKA_VARVID[k] for k in ALLIKA_NIMED.keys()],
            ),
        ),
        xOffset=alt.XOffset("allikas_nimi:N"),
        tooltip=[
            alt.Tooltip("laetud_kuupaev_str:O", title="Kuupäev"),
            alt.Tooltip("allikas_nimi:N", title="Allikas"),
            alt.Tooltip("kasutatavuse_pct:Q", title="Kasutatavus %", format=".1f"),
            alt.Tooltip("dokumente_kokku:Q", title="Dokumente"),
        ],
    )
    .properties(height=280)
)
st.altair_chart(kasutatavus, use_container_width=True)

# --- Mõõdik 3: peamised kvaliteedipuudused ---
st.subheader("Mõõdik 3 — peamised kvaliteedipuudused allika kohta")

if not docs.empty:
    docs["allikas_nimi"] = docs["allikas"].map(ALLIKA_NIMED).fillna(docs["allikas"])
    docs["puuduse_nimi"] = docs["peamine_pohjus"].map(PUUDUSE_NIMED).fillna(docs["peamine_pohjus"])
    tegelikud_puudused = [k for k in PUUDUSE_NIMED if k != "puudus_puudub"]
    puudused = (
        docs[docs["peamine_pohjus"].isin(tegelikud_puudused)]
        .groupby(["allikas_nimi", "puuduse_nimi"])
        .size()
        .reset_index(name="arv")
    )
    puuduste_diagramm = (
        alt.Chart(puudused)
        .mark_arc()
        .encode(
            theta=alt.Theta("arv:Q"),
            color=alt.Color("puuduse_nimi:N", title="Puudus"),
            tooltip=[
                alt.Tooltip("puuduse_nimi:N", title="Puudus"),
                alt.Tooltip("allikas_nimi:N", title="Allikas"),
                alt.Tooltip("arv:Q", title="Dokumente"),
            ],
        )
        .properties(height=250)
        .facet(
            facet=alt.Facet("allikas_nimi:N", title=None),
            columns=3,
        )
    )
    st.altair_chart(puuduste_diagramm, use_container_width=True)

# --- Päevatabel ---
st.subheader("Päevased näitajad")

st.dataframe(
    quality[[
        "allikas_nimi", "laetud_kuupaev", "dokumente_kokku",
        "sonu_kokku", "sonu_kasutatav",
        "kasutatavuse_pct", "liiga_lyhike_pct", "vale_keel_pct", "duplikaat_pct",
        "peamine_kvaliteedipuudus",
    ]].rename(columns={
        "allikas_nimi":             "Allikas",
        "laetud_kuupaev":           "Kuupäev",
        "dokumente_kokku":          "Dokumente",
        "sonu_kokku":               "Sõnu kokku",
        "sonu_kasutatav":           "Kasutatavaid sõnu",
        "kasutatavuse_pct":         "Kasutatavus %",
        "liiga_lyhike_pct":         "Lühike %",
        "vale_keel_pct":            "Vale keel %",
        "duplikaat_pct":            "Duplikaat %",
        "peamine_kvaliteedipuudus": "Peamine puudus",
    }),
    use_container_width=True,
    hide_index=True,
)

# --- Pipeline'i käivitused ---
with st.expander("Töövoo käivitused (viimased 20)"):
    st.dataframe(runs, use_container_width=True, hide_index=True)

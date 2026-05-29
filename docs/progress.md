# Edenemisraport

## Mis on valmis

- [x] Kõik kolm Airflow DAG-i käivituvad igapäevaselt ja toovad uusi andmeid
- [ ] Andmed laetakse staging kihti (`riigikogu_raw`, `rahvaalgatus_raw`, `wikipedia_raw`)
- [ ] dbt transformatsioonid toimivad otsast lõpuni (`stg_*` → `int_documents` → `fct_documents`, `mart_source_quality`)
- [ ] Andmekvaliteedi testid on rohelised
- [ ] Streamlit näidikulaud näitab kõiki kolme mõõdikut
- [ ] Uus keskkond on võimalik püsti panna README juhendi järgi (sh ajaloolised andmed)

## Järgmised sammud

- Ülejäänud dbt kvaliteeditestid lisada ja roheliseks saada
- Näidikulaua viimistlemine
- Kokkuvõte, puudused ja edasiarendused README-sse kirja panna

## Mis takistab

- Rahvaalgatus.ee API tagastab algatused ilma offseti toeta (ignoreerimine leitud
  ja parandatud — `limit=2000` toob kõik ~1100 algatust ühes päringus)
- Vikipeedia API piirab päringute sagedust (429 vead) — migratsiooniskript kasutab
  2-sekundilist pausi; igapäevane DAG töötab normaalselt

## Võimalikud edasiarendused

### Uued andmeallikad
- **Riigikogu eelnõud** — täistekst on `.docx` failides (`/api/volumes/drafts` → `/api/files/{uuid}/download`); vajab `python-docx` teeki
- **Riigikogu komisjonide protokollid** — saadaval PDF ja ASICE failidena (`/api/events?type=COMMITTEE`); vajab PDF-ist teksti ekstraktimist (`pdfplumber` vms)
- **Riigi Teataja** — õigusaktide täistekstid, avalik API olemas
- **ERR uudised** — RSS-voog, lihtne scraper

### Andmekvaliteedi täiustused
- **Täpsem keeletuvastus** — praegune `is_estonian` kontrollib lihtsalt eesti tähtede ja sõnade olemasolu; võiks kasutada `langdetect` või `lingua` teeki
- **Duplikaatide tuvastus allikate vahel** — praegu tuvastatakse ainult staging-sisesed duplikaadid (sama hash); ristallika duplikaadid jäävad märkamata
- **Teksti normaliseerimise samm** — HTML-jäägid, liigne whitespace, päised/jalused eemaldada enne kvaliteedikontrolli

### Näidikulaud
- **Dok_tyyp järgi filtreerimine** — näidata stenogramme ja eelnõusid eraldi (kui eelnõud lisatakse)
- **Ajavahemiku valik** — praegu näidatakse kogu ajalugu; lisada kuupäevafiltrid
- **Allalaadimislink** — võimaldada kasutajal eksportida filtreeritud andmed CSV-na

### Tehniline võlg
- Airflow DAG-id kasutavad `retries=1`; tootmiskeskkonnas soovitav `retries=3`
- `migrate_seed_to_staging.py` skript on ühekordne tööriist — võiks eemaldada pärast projekti lõppu

## Kontrollpunkt

Viimased töövoo käivitused ja nende tulemus:

```bash
docker compose exec analytics-db psql -U EKI -d eki_postgres -c "
SELECT source_name, DATE(fetched_at) as kuupaev, docs_added, status
FROM staging.pipeline_runs
WHERE status = 'success'
ORDER BY fetched_at DESC
LIMIT 10;"
```

Näidikulaud: http://localhost:8501

# Edenemisraport

## Mis on valmis

- [ ] Docker Compose käivitab kõik teenused
- [ ] Andmeid saadakse Riigikogu API-st kätte
- [ ] Andmed laetakse `raw` kihti
- [ ] Vähemalt üks transformatsioon toimib (stg_riigikogu)
- [ ] Vähemalt üks näidikulaud on nähtaval (Metabase)
- [ ] Vähemalt üks andmekvaliteedi test läbib (not_null, unique)
- [ ] Rahvaalgatus.ee API sissevõtt valmis
- [ ] Vikipeedia API sissevõtt valmis
- [ ] Kõik andmekvaliteedi testid rohelised

Riigikogu andmevoog töötab otsast lõpuni: API → raw → staging →
Metabase. Evelin on Rahvaalgatus.ee API sissevõtuga töös, Liis
Vikipeedia omaga. seeds/allikad.csv on valmis.

## Järgmised sammud

- Rahvaalgatus.ee ja Vikipeedia sissevõtt lõpetada ja staging
  mudelid lisada
- mart_allikate_maht ja mart_kvaliteet mudelid valmis teha
  (agregeeritud andmed dashboardi jaoks)
- Ülejäänud andmekvaliteedi testid lisada (keeletuvastus, värskus)
- Metabase dashboard täiendada kõigi 3 mõõdikuga
- Evelin teeb oma README ja testide osad valmis enne 1. juunit
  (puhkus 1.–7. juunil)

## Mis takistab

- Riigikogu API tagastab istungipäevadel rohkem andmeid kui muudel
  päevadel — värskuse test (< 48h) vajab kohandamist, et mitte
  valepositiivseid anda nädalavahetustel
- Vikipeedia API `continue`-parameeter vajab veel testimist,
  et leheküljed õigesti järjest kätte saada

## Kontrollpunkt

Käsk, millega saab kontrollida, et töövoog töötab:

```bash
docker compose exec pipeline python scripts/run_pipeline.py check
```

Oodatav tulemus: kõik kolm allikat väljastab viimase eduka
sissevõtu kellaaja ja kirjete arvu, nt:
  riigikogu      last_run=2026-05-30 06:00  rows=142
  rahvaalgatus   last_run=2026-05-30 06:00  rows=38
  wikipedia      last_run=2026-05-30 06:00  rows=215

-- Loob skeemid ja toorandmete tabelid.
-- dbt loob staging vaated, intermediate vaate ja marts tabelid ise,
-- kuid kolm raw-tabelit peavad enne olemas olema, et Airflow saaks neisse kirjutada.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Töövoo käivituste jälgimine (jagatud kõigi allikate vahel)
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id       uuid        PRIMARY KEY,
    fetched_at   timestamptz NOT NULL,
    source_name  text        NOT NULL,           -- 'riigikogu' | 'rahvaalgatus' | 'wikipedia'
    status       text        NOT NULL,           -- 'running' | 'success' | 'failed'
    docs_added   integer     NOT NULL DEFAULT 0, -- kirjete arv, mis sissevõtt lisas
    message      text
);

-- Riigikogu toorandmed (istungite dokumendid ja stenogrammid)
CREATE TABLE IF NOT EXISTS staging.riigikogu_raw (
    run_id        uuid         NOT NULL REFERENCES staging.pipeline_runs(run_id),
    doc_id        text         NOT NULL,         -- Riigikogu dokumendi unikaalne ID
    pealkiri      text,
    tekst         text,
    dok_tyyp      text,                          -- 'stenogramm' | 'eelnou' | jne
    avaldatud     timestamptz,
    allikas_url   text,
    laetud_kell   timestamptz  NOT NULL,
    PRIMARY KEY (doc_id)
);

-- Rahvaalgatus.ee toorandmed (algatuste täistekst + metaandmed)
CREATE TABLE IF NOT EXISTS staging.rahvaalgatus_raw (
    run_id        uuid         NOT NULL REFERENCES staging.pipeline_runs(run_id),
    doc_id        text         NOT NULL,         -- algatuse UUID API-st
    pealkiri      text,
    tekst         text,                          -- HTML scraperist eraldatud puhastatud tekst
    staatus       text,                          -- 'voting' | 'in_parliament' | ...
    avaldatud     timestamptz,
    allikas_url   text,
    laetud_kell   timestamptz  NOT NULL,
    PRIMARY KEY (doc_id)
);

-- Wikipedia toorandmed (artiklite täistekst)
CREATE TABLE IF NOT EXISTS staging.wikipedia_raw (
    run_id        uuid         NOT NULL REFERENCES staging.pipeline_runs(run_id),
    doc_id        text         NOT NULL,         -- artikli pageid + revid
    pealkiri      text,
    tekst         text,                          -- wikitext või extract
    revid         bigint,
    avaldatud     timestamptz,                   -- viimase muudatuse aeg
    allikas_url   text,
    laetud_kell   timestamptz  NOT NULL,
    PRIMARY KEY (doc_id)
);

CREATE INDEX IF NOT EXISTS riigikogu_raw_laetud_idx ON staging.riigikogu_raw (laetud_kell);
CREATE INDEX IF NOT EXISTS rahvaalgatus_raw_laetud_idx ON staging.rahvaalgatus_raw (laetud_kell);
CREATE INDEX IF NOT EXISTS wikipedia_raw_laetud_idx ON staging.wikipedia_raw (laetud_kell);

-- Wikipedia jäetakse välja — sama artikli eri revisioonid jagavad URL-i aga saavad erineva doc_id
ALTER TABLE staging.riigikogu_raw    ADD CONSTRAINT IF NOT EXISTS riigikogu_raw_url_key    UNIQUE (allikas_url);
ALTER TABLE staging.rahvaalgatus_raw ADD CONSTRAINT IF NOT EXISTS rahvaalgatus_raw_url_key UNIQUE (allikas_url);

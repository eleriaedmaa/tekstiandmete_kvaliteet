#rahvaalgatus.ee API dokumentatsioon: https://app.swaggerhub.com/apis-docs/rahvaalgatus/rahvaalgatus/1
#Põhiandmevoog: algatuste metaandmed (API) ja täistekst (scraper)
#API: ainult initatives: id, for, title, phase, signinEndsAt, signatureCount, signatureThreshold
#API päringust jäid välja statistics ja initiative events. Initiative events ei ole API kaudu filtreeritavad

import httpx
from bs4 import BeautifulSoup
import time

BASE_URL = "https://rahvaalgatus.ee"


def get_all_initiative_ids() -> list:
    """Kõikide algatuste (initiatives) ID-d lehekülgede (pagination) kaupa."""
    all_ids = []
    seen_ids = set()  #duplikaadid
    offset = 0
    limit = 50

    while True:
        r = httpx.get(
            f"{BASE_URL}/initiatives",
            headers={"Accept": "application/vnd.rahvaalgatus.initiative+json; v=1"},
            params={
                "order": "-createdAt",
                "limit": limit,
                "offset": offset,
            },
            timeout=10,
        )
        r.raise_for_status()
        batch = r.json()

        # Peatu, kui partiid (batch) ei leitud
        if not batch:
            break

        # Peatu, kui leiti ID, mis juba eksisteerib
        new_ids = [item["id"] for item in batch if item["id"] not in seen_ids]
        if not new_ids:
            print("Uusi ID-sid selles partiis ei leitud.")
            break

        for id in new_ids:
            seen_ids.add(id)
            all_ids.append(id)

        print(f"Siiani leitud {len(all_ids)} algatust...")

        if len(batch) < limit:
            break

        offset += limit
        time.sleep(0.5)

    return all_ids

def get_initiative_metadata(initiative_id) -> dict:
    r = httpx.get(
        f"{BASE_URL}/initiatives/{initiative_id}",
        headers={"Accept": "application/vnd.rahvaalgatus.initiative+json; v=1"},
        timeout=10,
        follow_redirects=True,
    )
    r.raise_for_status()
    return r.json()


def get_estonian_text(initiative_id) -> dict:
    url = f"{BASE_URL}/initiatives/{initiative_id}"
    r = httpx.get(
        url,
        timeout=10,
        follow_redirects=True,
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Tõmba pealkiri
    title = soup.find("h1")
    title_text = title.get_text(strip=True) if title else ""

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

    return {
        "title": title_text,
        "url": url,
        "text": "\n\n".join(body_paragraphs),
    }


def fetch_full_initiative(initiative_id):
    """Liida metaandmed, täistekstid ja prindi tulemus."""
    meta = get_initiative_metadata(initiative_id)
    html_data = get_estonian_text(initiative_id)

    # --- Prindi metaandmed ---
    print("\n" + "=" * 60)
    print("ALGATUSE METAANDMED")
    print("=" * 60)
    print(f"ID:              {meta.get('id')}")
    print(f"Kellele:         {meta.get('for')}")
    print(f"Pealkiri:        {meta.get('title')}")
    print(f"Etapp:           {meta.get('phase')}")
    print(f"Tähtaeg:         {meta.get('signingEndsAt')}")
    print(f"Allkirjad:       {meta.get('signatureCount')}")
    print(f"Künnis:          {meta.get('signatureThreshold')}")

    # --- Prindi täistekst---
    print("\n" + "=" * 60)
    print("TÄISTEKST EESTI KEELES")
    print("=" * 60)
    print(html_data["text"])


def fetch_all_initiatives():
    """Tõmba ja prindi kõik algatused koos metaandmete ja täistekstidega."""
    print("Tõmban kõikide algatuste ID-sid...")
    ids = get_all_initiative_ids()
    print(f"\nKokku leitud {len(ids)} algatust")
    print("Alustan...\n")

    for i, initiative_id in enumerate(ids, 1):
        print(f"\n[{i}/{len(ids)}] Tõmban algatust {initiative_id}...")
        try:
            fetch_full_initiative(initiative_id)
        except Exception as e:
            print(f"  ERROR algatusega {initiative_id}: {e}")

        time.sleep(1)  # 1 sekund iga päringu vahel


if __name__ == "__main__":
    fetch_all_initiatives()
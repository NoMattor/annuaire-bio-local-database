#!/usr/bin/env python3
"""
scraper_producteur.py

Scraper dédié aux producteurs locaux/bio en Belgique.
Utilise l'API Google Places pour rechercher uniquement les lieux correspondant
aux mots-clés définis dans `keyword_producteurs.txt`.

Lit :
 - cities.txt               (liste des villes francophones)
 - keyword_producteurs.txt  (mots-clés de producteurs)

Écrit un CSV : data/producteurs.csv
avec pour chaque producteur :
 - name, address, city, postal_code, rating, user_ratings_total, types, maps_url
 - matched_keyword

Déduplication par place_id pour ne conserver qu'une occurence par producteur.
"""

import time
import csv
from pathlib import Path
import os
import re
import requests
from dotenv import load_dotenv, find_dotenv

# ---------------------------------------------------------------------------
# Chargement de la clé API
# ---------------------------------------------------------------------------
env_path = find_dotenv()
if env_path:
    load_dotenv(env_path, override=True)
else:
    print(f"⚠️ .env non trouvé ; assurez-vous que votre clé est dans le fichier .env.")
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise SystemExit("⚠️  GOOGLE_API_KEY manquant. Ajoutez votre clé dans .env.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_list(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")]


def parse_city_postal(address: str) -> tuple[str, str]:
    # Extrait code postal et ville
    m = re.search(r",\s*(\d{4,5})\s+([^,]+)", address)
    if m:
        return m.group(2).strip(), m.group(1).strip()
    return "", ""


def build_maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def places_text_search(query: str) -> list[dict]:
    """Interroge l'API Google Places Text Search et gère la pagination."""
    endpoint = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": API_KEY}
    results = []
    while True:
        resp = requests.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        token = data.get("next_page_token")
        if not token:
            break
        time.sleep(2.1)
        params = {"pagetoken": token, "key": API_KEY}
    return results

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    root = Path(__file__).resolve().parent.parent
    cities_file = root / "cities.txt"
    keywords_file = root / "keyword_producteurs.txt"
    output_csv = root / "data" / "producteurs.csv"

    # Vérification
    for p in (cities_file, keywords_file):
        if not p.exists():
            raise SystemExit(f"❌ Fichier manquant : {p}")

    cities = read_list(cities_file)
    keywords = read_list(keywords_file)

    # Prépare sortie
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "name", "address", "city", "postal_code",
        "rating", "user_ratings_total", "types", "maps_url",
        "matched_keyword"
    ]
    # CSV header
    if not output_csv.exists():
        with output_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    seen = {}  # place_id -> True

    # Parcours
    for city in cities:
        print(f"=== Ville : {city} ===")
        for kw in keywords:
            print(f"→ Recherche : {kw} in {city}")
            results = places_text_search(f"{kw} in {city}")
            for place in results:
                pid = place.get("place_id")
                if not pid or pid in seen:
                    continue
                seen[pid] = True
                name = place.get("name", "").strip()
                address = place.get("formatted_address", "").strip()
                city_name, postal = parse_city_postal(address)
                rating = place.get("rating", "")
                reviews = place.get("user_ratings_total", "")
                types = "|".join(place.get("types", []))
                maps_url = build_maps_url(pid)

                row = {
                    "name": name,
                    "address": address,
                    "city": city_name,
                    "postal_code": postal,
                    "rating": rating,
                    "user_ratings_total": reviews,
                    "types": types,
                    "maps_url": maps_url,
                    "matched_keyword": kw,
                }
                with output_csv.open("a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)
            time.sleep(1)

    print(f"\n✅ Scraping terminé, résultats dans : {output_csv}")


if __name__ == "__main__":
    main()

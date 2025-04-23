#!/usr/bin/env python3
"""
scraper_producteur.py

Scraper dédié aux producteurs (fermes) locaux/bio en Belgique.
Utilise l'API Google Places pour rechercher uniquement les producteurs
en s'appuyant sur des types et mots-clés stricts.

Lit :
 - cities.txt               (liste des villes francophones)
 - keyword_producteurs.txt  (mots-clés de producteurs)

Écrit un CSV : data/producteurs.csv
avec pour chaque producteur :
 - name, address, city, postal_code, rating, user_ratings_total, types, maps_url
 - matched_keyword

Déduplication par place_id pour ne conserver qu'une occurrence par producteur.
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
    print("⚠️ .env non trouvé ; assurez-vous que votre clé est dans le fichier .env.")
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise SystemExit("⚠️  GOOGLE_API_KEY manquant. Ajoutez votre clé dans .env.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_list(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")]


def parse_city_postal(address: str) -> tuple[str, str]:
    m = re.search(r",\s*(\d{4,5})\s+([^,]+)", address)
    if m:
        return m.group(2).strip(), m.group(1).strip()
    return "", ""


def build_maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def places_text_search(query: str) -> list[dict]:
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

# Types Google à considérer comme ferme/producteur
PRODUCER_TYPES = {"farm", "farmers_market"}
# Mots-clés dans le nom garantissant un producteur (plus stricts)
PRODUCER_NAME_KEYWORDS = [
    "maraicher", "maraîcher",          # légumes/fruits
    "miellerie",                        # miel
    "apiculteur",                       # miel
    "élevage",                          # viande, œufs, volaille
    "fermier",                          # ferme générale
    "volaille",                         # poulet, œufs
    "marché fermier",                   # marché des producteurs
    "producteur"                        # générique producteur
    "ferme",                            # ferme générale

]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    root = Path(__file__).resolve().parent.parent
    cities_file = root / "cities.txt"
    keywords_file = root / "keyword_producteurs.txt"
    output_csv = root / "data" / "producteurs.csv"

    for p in (cities_file, keywords_file):
        if not p.exists():
            raise SystemExit(f"❌ Fichier manquant : {p}")

    cities = read_list(cities_file)
    keywords = read_list(keywords_file)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "name", "address", "city", "postal_code",
        "rating", "user_ratings_total", "types", "maps_url",
        "matched_keyword"
    ]
    if not output_csv.exists():
        with output_csv.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

    seen = set()

    for city in cities:
        print(f"=== Ville : {city} ===")
        for kw in keywords:
            print(f"→ Recherche : {kw} in {city}")
            results = places_text_search(f"{kw} in {city}")
            for place in results:
                pid = place.get("place_id")
                if not pid or pid in seen:
                    continue
                types_list = place.get("types", [])
                # Vérification type ou nom
                name = place.get("name", "").strip()
                name_l = name.lower()
                if not (set(types_list) & PRODUCER_TYPES or
                        any(key in name_l for key in PRODUCER_NAME_KEYWORDS)):
                    continue
                seen.add(pid)

                address = place.get("formatted_address", "").strip()
                city_name, postal = parse_city_postal(address)
                rating = place.get("rating", "")
                reviews = place.get("user_ratings_total", "")
                types = "|".join(types_list)
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

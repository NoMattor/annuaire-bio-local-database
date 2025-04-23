#!/usr/bin/env python3
"""
scrape_produits_bio.py

Module pour extraire via Google Places API un annuaire de magasins/ferm...
Expose une fonction `run_scraper(keywords, location, output_path)` pour
être importée et pilotée depuis un autre script.
"""

import os
import time
import argparse
import csv
from pathlib import Path

import requests
from dotenv import load_dotenv, find_dotenv

# -----------------------------------------------------------------------------
# Chargement du .env
# -----------------------------------------------------------------------------
env_path = find_dotenv()
if env_path:
    load_dotenv(env_path, override=True)
else:
    print("⚠️  Aucun fichier .env trouvé (looked at {}).".format(env_path))

# -----------------------------------------------------------------------------
# Constantes de filtrage
# -----------------------------------------------------------------------------
EXCLUDE_TYPES = {
    "courthouse",
    "local_government_office",
}

# Mots interdits dans le nom (lowercase)
EXCLUDE_NAME_KEYWORDS = {
    "tribunal",
    "palais",
    ""
}

API_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"


# -----------------------------------------------------------------------------
# Helpers internes
# -----------------------------------------------------------------------------
def load_api_key() -> str:
    """Lit la clé depuis GOOGLE_API_KEY ou quitte."""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise SystemExit("⚠️  GOOGLE_API_KEY manquant. Vérifiez votre .env.")
    return api_key


def fetch_places_for_keyword(keyword: str, location: str, api_key: str) -> list:
    """
    Interroge l'API Text Search pour un mot‑clé + lieu,
    gère la pagination et renvoie la liste brute de lieux.
    """
    params = {
        "query": f"{keyword} in {location}",
        "key": api_key,
    }
    all_results = []

    while True:
        resp = requests.get(API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        all_results.extend(data.get("results", []))

        next_token = data.get("next_page_token")
        if not next_token:
            break

        # Respect minimum 2s avant d'utiliser next_page_token
        time.sleep(2.1)
        params = {
            "pagetoken": next_token,
            "key": api_key,
        }

    return all_results


def parse_city_postal(formatted_address: str) -> tuple:
    """
    Extrait approximativement la ville et le code postal
    depuis formatted_address (split sur les virgules).
    """
    parts = [p.strip() for p in formatted_address.split(",")]
    if len(parts) >= 2:
        city_post = parts[-2]
        tokens = city_post.split()
        postal_code = tokens[0]
        city = " ".join(tokens[1:]) if len(tokens) > 1 else ""
    else:
        city = ""
        postal_code = ""
    return city, postal_code


def extract_place_data(place: dict) -> dict:
    """Formate un dict de l'API en dict plat pour le CSV."""
    place_id = place.get("place_id", "")
    name = place.get("name", "").strip()
    address = place.get("formatted_address", "").strip()
    city, postal_code = parse_city_postal(address)
    rating = place.get("rating", "")
    reviews = place.get("user_ratings_total", "")
    types = place.get("types", [])

    return {
        "place_id": place_id,
        "name": name,
        "address": address,
        "city": city,
        "postal_code": postal_code,
        "rating": rating,
        "user_ratings_total": reviews,
        "types": types,
        "maps_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    }


# -----------------------------------------------------------------------------
# Fonction principale à importer
# -----------------------------------------------------------------------------
def run_scraper(
    keywords: list[str],
    location: str,
    output_path: str
) -> int:
    """
    Lance le scraping pour une liste de keywords et un lieu,
    écrit le CSV à output_path. Retourne le nombre de lignes produites.
    """
    api_key = load_api_key()
    all_places = []

    for kw in keywords:
        print(f"→ Recherche : {kw} in {location}")
        results = fetch_places_for_keyword(kw, location, api_key)
        all_places.extend(results)
        # Petite pause entre deux recherches pour ne pas dépasser la QPS
        time.sleep(1.0)

    # Déduplication par place_id (on conserve la première occurrence)
    unique = {}
    for p in all_places:
        pid = p.get("place_id")
        if pid and pid not in unique:
            unique[pid] = p

    # Filtrage par type et par mots dans le nom
    filtered = []
    for raw in unique.values():
        data = extract_place_data(raw)
        # Exclusion si type black-listé
        if set(data["types"]) & EXCLUDE_TYPES:
            continue
        # Exclusion si nom contient un mot interdit
        low_name = data["name"].lower()
        if any(k in low_name for k in EXCLUDE_NAME_KEYWORDS):
            continue
        filtered.append(data)

    # Préparation du dossier de sortie
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # Écriture CSV
    fieldnames = [
        "name",
        "address",
        "city",
        "postal_code",
        "rating",
        "user_ratings_total",
        "types",
        "maps_url",
    ]
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in filtered:
            # jointure des types par pipe
            row["types"] = "|".join(row["types"])
            writer.writerow({k: row[k] for k in fieldnames})

    return len(filtered)


# -----------------------------------------------------------------------------
# Interface CLI conservée
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Google Places pour magasins/ferm..."
    )
    parser.add_argument(
        "-k", "--keywords",
        nargs="+", required=True,
        help="Liste de mots-clés (ex. magasin bio marché fermier)"
    )
    parser.add_argument(
        "-l", "--location",
        required=True,
        help="Zone géographique (ex. Namur, Belgium)"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Chemin vers le fichier CSV de sortie"
    )

    args = parser.parse_args()
    count = run_scraper(args.keywords, args.location, args.output)
    print(f"✅ {count} lignes sauvegardées dans {args.output}")

#!/usr/bin/env python3
"""
Google Places Bio Directory Scraper
----------------------------------

Recherche automatiquement les magasins bio, fermes et marchés locaux via l’API
Google Places (Text Search) puis exporte un CSV exploitable dans le dossier
«data» à la racine du projet.

Dépendances :
  * requests
  * python-dotenv

Installation :
  pip install requests python-dotenv

Configuration:
  Créez un fichier `.env` à la racine du projet (annuaire-bio-local-database/.env) :
      GOOGLE_API_KEY=VOTRE_CLÉ_ICI


Exemple d’exécution depuis le dossier scripts:
  cd annuaire-bio-local-database/scripts
  python3 scrape_one_by_one.py \
    -k "magasin bio" "marché fermier" "circuit court" \
    -l "Namur, Belgium" \
    -o namur_places.csv  # enregistré dans ../data/namur_places.csv

"""

from __future__ import annotations
import os
import time
import csv
import re
import argparse
from typing import List, Dict, Iterable, Set
from pathlib import Path

import requests
from dotenv import load_dotenv, find_dotenv

# ---------------------------------------------------------------------------
# Configuration des fichiers et chargement du .env
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Recherche automatique du .env dans les répertoires parents
env_path = find_dotenv(filename=".env", raise_error_if_not_found=False)
if not env_path:
    print(f"⚠️  .env non trouvé dans {PROJECT_ROOT} ou ses dossiers parents.")
else:
    print(f"Chargement des variables d'environnement depuis : {env_path}")
# Force le chargement depuis le fichier .env
load_dotenv(env_path, override=True)

# Création automatique du dossier data
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_OUTPUT = DATA_DIR / "places.csv"

# Types Google Places à exclure (fausses correspondances comme tribunal)
EXCLUDE_TYPES: Set[str] = {
    "courthouse",
    "local_government_office",
}

# Mots interdits dans le nom pour filtrer encore plus
EXCLUDE_NAME_KEYWORDS = ["tribunal", "palais"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_address(address: str) -> tuple[str, str]:
    m = re.search(r",\s*(\d{4,5})\s+([^,]+)", address)
    return (m.group(1).strip(), m.group(2).strip()) if m else ("", "")

def build_maps_link(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"

# ---------------------------------------------------------------------------
# Google Places logic
# ---------------------------------------------------------------------------
def places_text_search(query: str, api_key: str) -> List[Dict]:
    endpoint = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": api_key}
    all_results: List[Dict] = []
    while True:
        resp = requests.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))

        token = data.get("next_page_token")
        if not token:
            break
        time.sleep(2.1)
        params = {"pagetoken": token, "key": api_key}

    return all_results


def scrape(keywords: Iterable[str], location: str, api_key: str) -> Iterable[Dict]:
    seen: Dict[str, Dict] = {}

    for kw in keywords:
        query = f"{kw} in {location}"
        print(f"→ Recherche : {query}")
        for place in places_text_search(query, api_key):
            pid = place.get("place_id")
            if not pid or pid in seen:
                continue

            # Filtrage par type (exclut tribunaux, administrations...)
            types = set(place.get("types", []))
            if types & EXCLUDE_TYPES:
                continue

            # Filtrage par mot dans le nom
            name_lower = place.get("name", "").lower()
            if any(kw in name_lower for kw in EXCLUDE_NAME_KEYWORDS):
                continue

            addr = place.get("formatted_address", "")
            postal, city = parse_address(addr)

            seen[pid] = {
                "name": place.get("name", ""),
                "address": addr,
                "city": city,
                "postal_code": postal,
                "rating": place.get("rating", ""),
                "reviews": place.get("user_ratings_total", ""),
                "types": "|".join(place.get("types", [])),
                "maps_url": build_maps_link(pid),
            }
        # Pause pour respecter la QPS
        time.sleep(1)

    return seen.values()

# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------
def write_csv(rows: Iterable[Dict], output: Path) -> None:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["name","address","city","postal_code","rating","reviews","types","maps_url"]
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"\n✅ {sum(1 for _ in rows)} lignes sauvegardées dans {output}\n")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    
    api_key = os.getenv("GOOGLE_API_KEY")  # Assurez-vous que votre .env contient GOOGLE_API_KEY
    
    if not api_key:
        raise SystemExit("⚠️  GOOGLE_PLACES_API_KEY manquant. Vérifiez votre .env.")

    parser = argparse.ArgumentParser(description="Scrape les locaux bio via Google Places")
    parser.add_argument("-k","--keywords",nargs="+",required=True,help="Mots-clés")
    parser.add_argument("-l","--location",required=True,help="Ville ou lat,lng")
    parser.add_argument("-o","--output",type=Path,default=DEFAULT_OUTPUT,help="CSV (relatif à data/)")
    args = parser.parse_args()

    rows = list(scrape(args.keywords, args.location, api_key))
    out = args.output if args.output.is_absolute() else DATA_DIR / args.output
    write_csv(rows, out)

if __name__ == "__main__":
    main()

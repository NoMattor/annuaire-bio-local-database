#!/usr/bin/env python3
"""
run_all_scraper.py

Orchestre le scraping sur plusieurs villes et catégories,
et ajoute les nouveaux lieux au CSV existant dès la fin de chaque ville.

Lit les fichiers :
 - cities.txt        (liste des villes)
 - keywords.txt      (liste des mots-clés)
 - categories.txt    (mapping mot-clé → place_category)

Écrit un CSV agrégé : data/all_places.csv
avec pour chaque lieu :
 - name, address, city, postal_code, rating, user_ratings_total, types, maps_url
 - matched_keywords (plusieurs)
 - place_category  (plusieurs)
 - product_category (plusieurs)
En mode incrémental : n'ajoute que les lieux dont maps_url n'est pas déjà présent,
et écrit immédiatement après chaque ville pour éviter la perte de données.
"""

import time
import csv
from pathlib import Path

from scrape_produits_bio import load_api_key, fetch_places_for_keyword, extract_place_data


def read_list(path: Path) -> list[str]:
    return [l.strip() for l in path.read_text(encoding="utf-8").splitlines()
            if l.strip() and not l.strip().startswith("#")]


def read_mapping(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        txt = line.strip()
        if not txt or txt.startswith("#"): continue
        parts = txt.split(",", 1)
        if len(parts) != 2: continue
        key, val = parts
        mapping[key.strip().lower()] = val.strip()
    return mapping


def main():
    root = Path(__file__).resolve().parent.parent
    cities_file   = root / "cities.txt"
    keywords_file = root / "keywords.txt"
    cat_file      = root / "categories.txt"
    output_csv    = root / "data" / "all_places.csv"

    # Vérification
    for p in (cities_file, keywords_file, cat_file):
        if not p.exists():
            raise SystemExit(f"❌ Fichier manquant: {p}")

    cities    = read_list(cities_file)
    keywords  = read_list(keywords_file)
    place_map = read_mapping(cat_file)

    # Mapping produit → catégorie produit (lowercased keys)
    product_map: dict[str, str] = {
        "ferme œufs bio":          "Produits frais de saison",
        "ferme lait bio":          "Produits laitiers",
        "producteur légumes bio":  "Fruits & Légumes",
        "producteur fruits bio":   "Fruits & Légumes",
        "primeurs bio":            "Fruits & Légumes",
        "élevage vente viande vache":   "Viandes & Poissons",
        "élevage vente viande poulet":  "Viandes & Poissons",
        "boulangerie bio":         "Céréales & Pain",
        "charcuterie bio":         "Viandes & Poissons",
        "boucherie bio":           "Viandes & Poissons",
        "miel bio":                "Miels artisanaux et locaux",
        "ferme miel":              "Miels artisanaux et locaux",
        "ferme apiculteur":        "Miels artisanaux et locaux",
    }
    product_map = {k.lower():v for k,v in product_map.items()}

    # Préparation CSV
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "name","address","city","postal_code",
        "rating","user_ratings_total","types","maps_url",
        "matched_keywords","place_category","product_category"
    ]
    if not output_csv.exists():
        with output_csv.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

    api_key = load_api_key()

    # URLs déjà existantes
    existing_urls: set[str] = set()
    with output_csv.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            existing_urls.add(row.get("maps_url",""))

    # Scraping par ville
    for city in cities:
        print(f"=== Ville: {city} ===")
        city_data: dict[str, dict] = {}

        for kw in keywords:
            lckw = kw.lower()
            print(f"→ Recherche: {kw} in {city}")
            results = fetch_places_for_keyword(kw, city, api_key)
            for raw in results:
                data = extract_place_data(raw)
                url = data.get("maps_url")
                if not url or url in existing_urls:
                    continue
                # init
                if url not in city_data:
                    city_data[url] = data.copy()
                    city_data[url]["matched_keywords"] = set()
                    city_data[url]["place_category"] = set()
                    city_data[url]["product_category"] = set()
                # ajouter mots-clés et catégories
                city_data[url]["matched_keywords"].add(lckw)
                # place_category mappings
                plc = place_map.get(lckw)
                if plc:
                    city_data[url]["place_category"].add(plc)
                # product_category mappings
                pm = product_map.get(lckw)
                if pm:
                    city_data[url]["product_category"].add(pm)
            time.sleep(1)

        # écriture après la ville
        new_count = 0
        with output_csv.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            for entry in city_data.values():
                entry["matched_keywords"] = ";".join(sorted(entry["matched_keywords"]))
                entry["place_category"]   = ";".join(sorted(entry["place_category"]))
                entry["product_category"] = ";".join(sorted(entry["product_category"]))
                if isinstance(entry.get("types"), list):
                    entry["types"] = "|".join(entry["types"])
                clean = {k: entry.get(k,"") for k in fieldnames}
                writer.writerow(clean)
                existing_urls.add(entry.get("maps_url",""))
                new_count += 1
        print(f"  ✅ Ajouté {new_count} lieux pour {city}")

    print("\n✅ Scraping terminé, toutes les données sont dans", output_csv)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
run_all_scraper.py

Orchestre le scraping sur plusieurs villes et catégories.
Lit les fichiers :
 - cities.txt        (liste des villes)
 - keywords.txt      (liste des mots-clés à chercher)
 - categories.txt    (mapping mot-clé → place_category)
Écrit un CSV agrégé : data/all_places.csv
avec pour chaque lieu :
 - name, address, city, postal_code, rating, user_ratings_total, types, maps_url
 - matched_keywords
 - place_category
 - product_category
"""

import time
import csv
from pathlib import Path

from scrape_produits_bio import load_api_key, fetch_places_for_keyword, extract_place_data

def read_list(path: Path) -> list[str]:
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        txt = line.strip()
        if txt and not txt.startswith("#"):
            lines.append(txt)
    return lines

def read_mapping(path: Path) -> dict[str, str]:
    mapping = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        txt = line.strip()
        if not txt or txt.startswith("#"):
            continue
        key, val = txt.split(",", 1)
        mapping[key.strip()] = val.strip()
    return mapping

def main():
    # Chemins par défaut
    root = Path(__file__).resolve().parent.parent
    cities_file     = root / "cities.txt"
    keywords_file   = root / "keywords.txt"
    cat_file        = root / "categories.txt"
    output_csv      = root / "data" / "all_places.csv"

    # Lecture
    cities     = read_list(cities_file)
    keywords   = read_list(keywords_file)
    place_map  = read_mapping(cat_file)

    # Mapping produit → catégorie produit
    product_map = {
        "œufs bio":             "Produits frais de saison",
        "ferme avicole":        "Viandes & Poissons",
        "volailles bio":        "Viandes & Poissons",
        "lait bio":             "Produits laitiers",
        "fromagerie":           "Produits laitiers",
        "crèmerie bio":         "Produits laitiers",
        "yaourt bio":           "Produits laitiers",
        "légumes bio":          "Fruits & Légumes",
        "fruits bio":           "Fruits & Légumes",
        "primeurs bio":         "Fruits & Légumes",
        "fruiterie bio":        "Fruits & Légumes",
        "pain bio":             "Céréales & Pain",
        "boulangerie bio":      "Céréales & Pain",
        "viande bio":           "Viandes & Poissons",
        "boucherie bio":        "Viandes & Poissons",
        "charcuterie bio":      "Viandes & Poissons",
        "miel bio":             "Miels artisanaux et locaux",
        "apiculteur":           "Miels artisanaux et locaux",
        "épicerie fine bio":    "Produits transformés",
        "conserverie":          "Produits transformés",
        "huile d'olive bio":    "Produits transformés",
        "épices bio":           "Produits transformés",
        "jus de fruits bio":    "Produits transformés",
        "cave à vin bio":       "Produits transformés",
        "brasserie bio":        "Produits transformés",
        "distillerie bio":      "Produits transformés",
    }

    # Prépare la sortie
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Clé API
    api_key = load_api_key()

    # Stockage unique
    unique_places: dict[str, dict] = {}
    matched_kw:    dict[str, set[str]] = {}

    # Boucle villes × mots‑clés
    for city in cities:
        print(f"=== Ville : {city} ===")
        for kw in keywords:
            print(f"→ Recherche : {kw} in {city}")
            results = fetch_places_for_keyword(kw, city, api_key)
            for raw in results:
                pid = raw.get("place_id")
                if not pid:
                    continue
                if pid not in unique_places:
                    unique_places[pid] = extract_place_data(raw)
                    matched_kw[pid] = set()
                matched_kw[pid].add(kw)
            # Pause entre deux requêtes
            time.sleep(1.0)

    # Prépare les données finales
    rows = []
    for pid, data in unique_places.items():
        kws = matched_kw.get(pid, set())
        # Catégories de lieu
        p_cats = sorted({ place_map[k] for k in kws if k in place_map })
        # Catégories produits
        prod_cats = sorted({ product_map[k] for k in kws if k in product_map })

        data["matched_keywords"]  = ";".join(sorted(kws))
        data["place_category"]    = ";".join(p_cats)
        data["product_category"]  = ";".join(prod_cats)
        rows.append(data)

    # Écriture CSV
    fieldnames = [
        "name","address","city","postal_code",
        "rating","user_ratings_total","types","maps_url",
        "matched_keywords","place_category","product_category"
    ]
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            if isinstance(r["types"], list):
                r["types"] = "|".join(r["types"])
            writer.writerow(r)

    print(f"✅ {len(rows)} lieux agrégés dans {output_csv}")

if __name__ == "__main__":
    main()

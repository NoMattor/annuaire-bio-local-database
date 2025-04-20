#!/usr/bin/env python3
"""
run_all_scraper.py

Orchestre le scraping sur plusieurs villes en import direct de run_scraper().
Lit :
 - la liste des villes dans ../cities.txt
 - la liste des mots-clés dans ../keywords.txt
Écrit :
 - un CSV agrégé ../data/all_places.csv
"""

import argparse
import csv
from pathlib import Path

from scrape_produits_bio import run_scraper


def read_list(filepath: Path) -> list[str]:
    """
    Lit un fichier texte et renvoie
    la liste des lignes non vides,
    sans commentaires (#...).
    """
    lines = []
    for raw in filepath.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


def slugify(name: str) -> str:
    """
    Transforme un nom de ville en slug
    (minuscules, alnum→inchangés, autres→_).
    """
    return "".join(c.lower() if c.isalnum() else "_" for c in name)


def main():
    # Répertoires
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    default_cities = project_root / "cities.txt"
    default_keywords = project_root / "keywords.txt"
    default_output = project_root / "data" / "all_places.csv"

    # CLI
    parser = argparse.ArgumentParser(
        description="Orchestre run_scraper sur plusieurs villes"
    )
    parser.add_argument(
        "--cities-file", "-c",
        type=Path,
        default=default_cities,
        help="Fichier texte contenant la liste des villes (une par ligne)"
    )
    parser.add_argument(
        "--keywords-file", "-k",
        type=Path,
        default=default_keywords,
        help="Fichier texte contenant la liste des mots-clés (une par ligne)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=default_output,
        help="Chemin du CSV agrégé en sortie"
    )
    args = parser.parse_args()

    # Chargement des listes
    if not args.cities_file.exists():
        raise SystemExit(f"❌ cities-file introuvable : {args.cities_file}")
    if not args.keywords_file.exists():
        raise SystemExit(f"❌ keywords-file introuvable : {args.keywords_file}")

    cities = read_list(args.cities_file)
    keywords = read_list(args.keywords_file)

    # Prépare le dossier de sortie
    args.output.parent.mkdir(parents=True, exist_ok=True)

    first_write = True

    for city in cities:
        print(f"\n=== Traitement de la ville : {city} ===")
        slug = slugify(city)
        tmp_csv = project_root / "data" / f"{slug}.csv"

        # Appel du scraper
        count = run_scraper(keywords, city, str(tmp_csv))
        print(f"  → {count} lieux trouvés pour « {city} »")

        # Lecture et agrégation
        with tmp_csv.open(newline="", encoding="utf-8") as rf:
            reader = csv.reader(rf)
            rows = list(reader)

        if not rows:
            print(f"  ⚠️  Pas de données pour {city}, on passe.")
            continue

        header, *data_rows = rows

        mode = "w" if first_write else "a"
        with args.output.open(mode, newline="", encoding="utf-8") as wf:
            writer = csv.writer(wf)
            if first_write:
                writer.writerow(header)
                first_write = False
            writer.writerows(data_rows)

        # Nettoyage
        tmp_csv.unlink()

    print(f"\n✅ Agrégation terminée : {args.output}")

if __name__ == "__main__":
    main()

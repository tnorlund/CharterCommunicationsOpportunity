#!/usr/bin/env python3
"""
IMDB Actor Comparison Script

Compares movie ratings for Bill Murray and Owen Wilson using
IMDB Non-Commercial Datasets. Downloads the necessary TSV files,
processes them, and calculates average ratings for:
- Movies they starred in together
- Movies Bill Murray starred in alone
- Movies Owen Wilson starred in alone
"""

import os
import sys
from pathlib import Path
from typing import List, Optional, cast

import pandas as pd
import requests

# IMDB dataset URLs
IMDB_BASE_URL = "https://datasets.imdbws.com/"
DATASETS = {
    "name.basics": "name.basics.tsv.gz",
    "title.basics": "title.basics.tsv.gz",
    "title.principals": "title.principals.tsv.gz",
    "title.ratings": "title.ratings.tsv.gz",
}

# Actors to compare
ACTOR_1 = "Bill Murray"
ACTOR_2 = "Owen Wilson"


def download_file(url: str, dest_path: Path) -> None:
    """Download a file from URL to destination path."""
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = (downloaded / total_size) * 100
                print(f"\r  Progress: {pct:.1f}%", end="", flush=True)
    print()


def ensure_datasets(data_dir: Path) -> dict[str, Path]:
    """Ensure all required datasets are downloaded."""
    data_dir.mkdir(parents=True, exist_ok=True)
    paths = {}

    for name, filename in DATASETS.items():
        file_path = data_dir / filename
        if not file_path.exists():
            download_file(IMDB_BASE_URL + filename, file_path)
        else:
            print(f"Using cached: {filename}")
        paths[name] = file_path

    return paths


def load_tsv_gz(
    path: Path, usecols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Load a gzipped TSV file into a DataFrame."""
    print(f"Loading {path.name}...")
    return pd.read_csv(
        path,
        sep="\t",
        na_values="\\N",
        usecols=usecols,
        low_memory=False,
        dtype=str,
    )


def find_actor_nconst(name_basics: pd.DataFrame, actor_name: str) -> str:
    """Find the nconst (IMDB ID) for an actor by name."""
    matches = name_basics[name_basics["primaryName"] == actor_name]

    if matches.empty:
        raise ValueError(f"Actor '{actor_name}' not found in database")

    if len(matches) > 1:
        # Filter for actors/actresses if multiple matches
        actor_matches = matches[
            matches["primaryProfession"].str.contains(
                "actor|actress", na=False
            )
        ]
        if not actor_matches.empty:
            matches = actor_matches

    # Return the first match (usually the most prominent)
    nconst: str = str(matches.iloc[0]["nconst"])
    print(f"Found {actor_name}: {nconst}")
    return nconst


def get_actor_titles(principals: pd.DataFrame, nconst: str) -> set[str]:
    """Get all title IDs (tconst) for an actor."""
    actor_titles = principals[principals["nconst"] == nconst]["tconst"]
    return set(actor_titles)


def calculate_average_rating(
    tconsts: set[str], ratings: pd.DataFrame, title_basics: pd.DataFrame
) -> tuple[float, int, list[str]]:
    """Calculate average rating for a set of titles.

    Returns:
        Tuple of (average_rating, count, title_names).
    """
    if not tconsts:
        return 0.0, 0, []

    # Filter ratings for the given titles
    filtered = ratings[ratings["tconst"].isin(tconsts)].copy()
    filtered["averageRating"] = pd.to_numeric(
        filtered["averageRating"], errors="coerce"
    )

    # Get title names for display
    titles_df = title_basics[title_basics["tconst"].isin(tconsts)]
    title_names = titles_df["primaryTitle"].tolist()

    valid_ratings = filtered["averageRating"].dropna()
    if valid_ratings.empty:
        return 0.0, 0, title_names

    return valid_ratings.mean(), len(valid_ratings), title_names


def main() -> int:  # pylint: disable=too-many-locals,too-many-statements
    """Main entry point."""
    # Determine data directory
    data_dir = Path(os.environ.get("IMDB_DATA_DIR", "./imdb_data"))

    print("=" * 60)
    print("IMDB Actor Comparison: Bill Murray vs Owen Wilson")
    print("=" * 60)
    print()

    # Download/verify datasets
    print("Step 1: Ensuring datasets are available")
    print("-" * 40)
    paths = ensure_datasets(data_dir)
    print()

    # Load only required columns to save memory
    print("Step 2: Loading datasets")
    print("-" * 40)

    name_basics = load_tsv_gz(
        paths["name.basics"],
        usecols=["nconst", "primaryName", "primaryProfession"],
    )

    title_basics = load_tsv_gz(
        paths["title.basics"], usecols=["tconst", "primaryTitle", "titleType"]
    )

    # Filter to movies only
    title_basics = cast(
        pd.DataFrame,
        title_basics.loc[title_basics["titleType"] == "movie"].copy(),
    )
    movie_tconsts = set(title_basics["tconst"])

    principals = load_tsv_gz(
        paths["title.principals"], usecols=["tconst", "nconst", "category"]
    )

    # Filter principals to actors/actresses in movies
    principals = cast(
        pd.DataFrame,
        principals.loc[
            (principals["category"].isin(["actor", "actress"]))
            & (principals["tconst"].isin(movie_tconsts))
        ].copy(),
    )

    ratings = load_tsv_gz(
        paths["title.ratings"], usecols=["tconst", "averageRating", "numVotes"]
    )
    print()

    # Find actors
    print("Step 3: Finding actors")
    print("-" * 40)
    nconst_1 = find_actor_nconst(name_basics, ACTOR_1)
    nconst_2 = find_actor_nconst(name_basics, ACTOR_2)
    print()

    # Get their movies
    print("Step 4: Analyzing filmographies")
    print("-" * 40)
    titles_1 = get_actor_titles(principals, nconst_1)
    titles_2 = get_actor_titles(principals, nconst_2)

    # Compute sets
    both = titles_1 & titles_2
    only_1 = titles_1 - both
    only_2 = titles_2 - both

    print(f"{ACTOR_1} movies: {len(titles_1)}")
    print(f"{ACTOR_2} movies: {len(titles_2)}")
    print(f"Movies together: {len(both)}")
    print()

    # Calculate ratings
    print("Step 5: Calculating average ratings")
    print("-" * 40)

    avg_both, count_both, titles_both = calculate_average_rating(
        both, ratings, title_basics
    )
    avg_1, count_1, _ = calculate_average_rating(only_1, ratings, title_basics)
    avg_2, count_2, _ = calculate_average_rating(only_2, ratings, title_basics)
    print()

    # Print results
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print()

    if titles_both:
        print(f"Movies {ACTOR_1} and {ACTOR_2} starred in together:")
        for title in sorted(titles_both)[:10]:  # Show up to 10
            print(f"  - {title}")
        if len(titles_both) > 10:
            print(f"  ... and {len(titles_both) - 10} more")
        print()

    print("Average Ratings Comparison:")
    print("-" * 40)
    print(
        f"  {ACTOR_1} & {ACTOR_2} together: "
        f"{avg_both:.2f} ({count_both} movies)"
    )
    print(
        f"  {ACTOR_1} only:                 " f"{avg_1:.2f} ({count_1} movies)"
    )
    print(
        f"  {ACTOR_2} only:                 " f"{avg_2:.2f} ({count_2} movies)"
    )
    print()

    # Comparison analysis
    print("Analysis:")
    print("-" * 40)
    if avg_both > avg_1 and avg_both > avg_2:
        print("  Together: HIGHER ratings than solo work!")
    elif avg_both < avg_1 and avg_both < avg_2:
        print("  Together: LOWER ratings than solo work.")
    else:
        print("  Mixed: collaborations between individual averages.")

    diff_1 = avg_both - avg_1
    diff_2 = avg_both - avg_2
    print(f"  Difference from {ACTOR_1} solo: {diff_1:+.2f}")
    print(f"  Difference from {ACTOR_2} solo: {diff_2:+.2f}")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

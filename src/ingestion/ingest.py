"""
Stage 0 - Ingestion.

Downloads the Kaggle dataset locally (run this on your own machine, NOT in a
network-restricted sandbox) and lays it out the way the rest of the pipeline
expects: data/raw/yellow_tripdata_<year>-<month>.csv

Usage:
    python src/ingestion/ingest.py
"""
import shutil
import glob
import os
import kagglehub

from src.utils.spark_utils import load_config


def download_kaggle_dataset(slug: str) -> str:
    print(f"Downloading Kaggle dataset: {slug}")
    path = kagglehub.dataset_download(slug)
    print(f"Downloaded to cache at: {path}")
    return path


def stage_raw_files(cache_path: str, raw_dir: str):
    os.makedirs(raw_dir, exist_ok=True)
    # dhruvildave/new-york-city-taxi-trips-2019 ships one CSV per month, named
    # inconsistently release to release. Grab everything that looks like a
    # trip file and copy it into a flat, predictably-named raw/ folder.
    candidates = glob.glob(os.path.join(cache_path, "**", "*.csv"), recursive=True)
    if not candidates:
        raise FileNotFoundError(
            f"No CSVs found under {cache_path} - check the Kaggle download completed."
        )
    for src in candidates:
        dest_name = os.path.basename(src).lower().replace(" ", "_")
        dest = os.path.join(raw_dir, dest_name)
        shutil.copy2(src, dest)
        print(f"Staged {dest_name}")
    print(f"Staged {len(candidates)} file(s) into {raw_dir}")


if __name__ == "__main__":
    cfg = load_config()
    cache_path = download_kaggle_dataset(cfg["kaggle"]["dataset_slug"])
    stage_raw_files(cache_path, cfg["paths"]["raw_dir"])

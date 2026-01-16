#!/usr/bin/env python3
"""
Extract metadata from all README.yaml files in the NMRLipids Databank and save as Parquet.

This script:
- Walks through the NMRLipids /Simulations/ folder
- Finds all README.yaml files
- Loads metadata from each file
- Injects mandatory metadata: source="NMRLipids", crawling_date=today, licence="CC-BY 4.0"
- Saves two Parquet files:
    1) NMRLipids_files.parquet
       - Columns: source, ID, TRAJECTORY_SIZE, TRJLENGTH
    2) NMRLipids_metadata.parquet
       - Columns: ID, source, DOI, crawling_date, SOFTWARE, PUBLICATION, AUTHORS_CONTACT,
                  TYPEOFSYSTEM, SOFTWARE_VERSION, FF, FF_SOURCE, TEMPERATURE, NUMBER_OF_ATOMS,
                  DATEOFRUNNING

FIELD DESCRIPTIONS:
-------------------
ID: Unique ID number of the dataset
source: Source of the dataset (here: NMRLipids)
DOI: DOI where the raw data exists
crawling_date: Date when this metadata was collected
SOFTWARE: Software used for simulation
PUBLICATION: Related publication(s), optional
AUTHORS_CONTACT: Name and email of main author(s)
TYPEOFSYSTEM: System type (e.g., lipid bilayer)
SOFTWARE_VERSION: Version of the simulation software
FF: Force field used
FF_SOURCE: Source of the force field
TRAJECTORY_SIZE: Size of trajectory file (bytes)
TRJLENGTH: Length of trajectory (ps)
TEMPERATURE: Simulation temperature
NUMBER_OF_ATOMS: Number of atoms in the system
DATEOFRUNNING: Date when added to the Databank
"""

# METADATA
__authors__ = ("Pierre Poulain", "Salahudin Sheikh")
__contact__ = ("pierre.poulain@u-paris.fr", "sheikh@ibpc.fr")
__copyright__ = "AGPL-3.0 license"
__date__ = "2025"
__version__ = "1.0.0"

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from pydantic import BaseModel, field_validator

# -------------------------
# Configuration
# -------------------------
FILES_PARQUET = "nmrlipids_files.parquet"
METADATA_PARQUET = "nmrlipids_datasets.parquet"

FIELDS = [
    "ID",
    "DOI",
    "SOFTWARE",
    "PUBLICATION",
    "AUTHORS_CONTACT",
    "TYPEOFSYSTEM",
    "SOFTWARE_VERSION",
    "FF",
    "FF_SOURCE",
    "TRAJECTORY_SIZE",
    "TRJLENGTH",
    "TEMPERATURE",
    "NUMBER_OF_ATOMS",
    "DATEOFRUNNING",
]

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# -------------------------
# Pydantic model
# -------------------------
class NMRLipidsRecord(BaseModel):
    # Mandatory
    source: str = "NMRLipids"
    source_ID: int | str
    crawling_date: str
    licence: str = "CC-BY 4.0"

    # Optional metadata
    DOI: str | None = None
    SOFTWARE: str | None = None
    PUBLICATION: str | None = None
    AUTHORS_CONTACT: str | None = None
    TYPEOFSYSTEM: str | None = None
    SOFTWARE_VERSION: str | int | float | None = None
    FF: str | None = None
    FF_SOURCE: str | None = None
    TRAJECTORY_SIZE: int | float | None = None
    TRJLENGTH: int | float | None = None
    TEMPERATURE: int | float | None = None
    NUMBER_OF_ATOMS: int | float | None = None
    DATEOFRUNNING: str | None = None

    # Normalize numeric fields to string if needed
    @field_validator(
        "source_ID",
        "SOFTWARE_VERSION",
        "TRAJECTORY_SIZE",
        "TRJLENGTH",
        "TEMPERATURE",
        "NUMBER_OF_ATOMS",
        mode="before",
    )
    def normalize_to_string_or_none(cls, v):
        if v is None:
            return None
        return str(v)


# -------------------------
# Functions
# -------------------------
def find_all_readmes(sim_root: Path):
    """Return all README.yaml files under the Simulations folder."""
    readmes = list(sim_root.rglob("README.yaml"))
    logger.info(f"Found {len(readmes)} README.yaml files under {sim_root}")
    return readmes


def parse_readme_yaml(path: Path) -> dict | None:
    """Load README.yaml, validate metadata with Pydantic, return dict."""
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return None

    extracted = {field: data.get(field, None) for field in FIELDS}

    # Rename ID → source_ID
    extracted["source_ID"] = extracted.pop("ID", None)

    # Inject mandatory metadata
    extracted["source"] = "NMRLipids"
    extracted["crawling_date"] = datetime.now().date().isoformat()
    extracted["licence"] = "CC-BY 4.0"

    try:
        record = NMRLipidsRecord(**extracted)
        return record.model_dump()
    except Exception as e:
        logger.warning(f"Validation failed for {path}:\n{e}")
        return None


def save_parquet(records):
    """Save metadata into two Parquet files, filling missing fields with 'None'."""
    if not records:
        logger.warning("No metadata extracted. Nothing to save.")
        return

    df = pd.DataFrame(records)

    # -------- Files parquet --------
    df_files = df[["source", "source_ID", "TRAJECTORY_SIZE", "TRJLENGTH"]].copy()
    df_files.fillna("None", inplace=True)
    df_files.to_parquet(FILES_PARQUET, index=False)
    logger.info(f"Saved file info → {FILES_PARQUET}")

    # -------- Metadata parquet --------
    df_meta = df[
        [
            "source",
            "source_ID",
            "DOI",
            "crawling_date",
            "PUBLICATION",
            "AUTHORS_CONTACT",
            "TYPEOFSYSTEM",
            "SOFTWARE",
            "SOFTWARE_VERSION",
            "FF",
            "FF_SOURCE",
            "TEMPERATURE",
            "NUMBER_OF_ATOMS",
            "DATEOFRUNNING",
        ]
    ].copy()

    # Fill missing fields with "None"
    df_meta.fillna("None", inplace=True)
    df_meta.to_parquet(METADATA_PARQUET, index=False)
    logger.info(f"Saved metadata → {METADATA_PARQUET}")


# -------------------------
# Main execution
# -------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract metadata from the NMRLipids Databank"
    )
    parser.add_argument(
        "--sim-folder",
        type=Path,
        required=True,
        help="Path to BilayerData/Simulations directory",
    )
    args = parser.parse_args()

    sim_folder = args.sim_folder

    if not sim_folder.exists():
        logger.error(f"Simulations folder not found: {sim_folder}")
        return

    readme_files = find_all_readmes(sim_folder)

    metadata_records = []
    for readme in readme_files:
        meta = parse_readme_yaml(readme)
        if meta:
            metadata_records.append(meta)

    logger.info(f"Extracted metadata from {len(metadata_records)} simulations.")
    save_parquet(metadata_records)


if __name__ == "__main__":
    main()

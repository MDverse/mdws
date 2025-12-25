#!/usr/bin/env python3

"""


INSTALL them in myenv if there was some error in running this and some packages are not installed #######(pip install "pydantic>=2.0" httpx pandas pyarrow tenacity)######


ATLAS MD scraper — cleaned, strict Pydantic-integrated single-file script.

- Strict typing (no "null" strings)
- Pydantic validation for external data
- Async HTTP fetching with retries
- Concurrency control and polite delays
- Atomic parquet writes (two files: files + metadata)

Scrape molecular dynamics metadata and files from ATLAS of proTein moLecular dynAmicS .

This script fetches molecular dynamics (MD) metadata from the ATLAS repository (https://www.dsimb.inserm.fr/ATLAS/).
It collects metadata such as dataset names, organisms, sequences, authors, DOIs, and file information for protein MDs.

The scraped data is saved locally in Parquet format:
    - "ATLAS_files.parquet" : file-level metadata (file names, file sizes, number of files)
    - "ATLAS_metadata.parquet" : dataset metadata (source, title, organism, DOI, sequence, etc.)

Usage :
=======
    python3 fetch_atlas.py

Ensure required packages are installed:
    - httpx
    - pandas
    - pyarrow


FIELD DESCRIPTIONS:
-------------------
source: Source of the dataset (here: ATLAS)
source_id: Unique identifier for the PDB chain or entry
data_set_url: URL to the dataset metadata API endpoint
title: Protein name or dataset title
organism: Organism from which the protein originates
length: Length of the protein sequence (number of residues)
sequence: Amino acid sequence of the protein
crawling_date: Date when this metadata was collected
date_creation: Original creation date of the dataset (if available)
date_last_modification: Last modification date of the dataset (if available)
nb_files: Number of files available for the dataset
file_names: Comma-separated list of available file names
file_sizes: Comma-separated list of file sizes corresponding to file_names
license: License under which the dataset is shared
authors: Names of authors or contributors
doi: DOI of the publication describing the dataset

"""



import asyncio
import logging
import re
import shutil
import tempfile
import time
from asyncio import Semaphore
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Annotated

import httpx
import pandas as pd
from pydantic import BaseModel, Field, HttpUrl, StringConstraints, field_validator, conint, constr
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


# ------------------------
# Metadata / Authors
# ------------------------
__authors__ = ("Pierre Poulain", "Salahudin Sheikh")
__contact__ = ("pierre.poulain@u-paris.fr", "sheikh@ibpc.fr")
__copyright__ = "AGPL-3.0"
__date__ = "2025"
__version__ = "1.0.0"

# ------------------------
# Configuration
# ------------------------
HTML_LIST_URL: str = "https://www.dsimb.inserm.fr/ATLAS/"
API_BASE: str = "https://www.dsimb.inserm.fr/ATLAS/api/ATLAS/metadata/"
BASE_URL: str = "https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/"

FILES_PARQUET = "ATLAS_files.parquet"
METADATA_PARQUET = "ATLAS_metadata.parquet"
OUTPUT_DIR = "output"

MAX_CONCURRENT_REQUESTS: int = 10
REQUEST_DELAY: float = 0.05  # polite delay (seconds)
HTTP_TIMEOUT: float = 30.0
RETRY_ATTEMPTS: int = 3

HEADERS = {
    "User-Agent": "atlas-scraper/1.0 (+https://example.org)",
}

# ------------------------
# Logging
# ------------------------
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("atlas_scraper")

# ------------------------
# Pydantic models
# ------------------------

# Strict sequence: only amino-acid letters (uppercase)
SequenceStr = Annotated[str, StringConstraints(pattern=r"^[ACDEFGHIKLMNPQRSTVWY]+$")]

class DatasetRecord(BaseModel):
    source: constr(min_length=1) = "ATLAS"
    source_id: constr(min_length=1)
    data_set_url: HttpUrl
    title: constr(min_length=1)
    organism: Optional[constr(min_length=1)] = None
    length: Optional[conint(ge=0)] = None
    sequence: Optional[SequenceStr] = None

    crawling_date: constr(min_length=1)
    date_creation: Optional[constr(min_length=1)] = None
    date_last_modification: Optional[constr(min_length=1)] = None

    nb_files: conint(ge=0) = 0
    file_names: List[constr(min_length=1)] = Field(default_factory=list)
    file_sizes: List[Optional[constr(min_length=1)]] = Field(default_factory=list)

    license: Optional[constr(min_length=1)] = None
    authors: Optional[constr(min_length=1)] = None
    doi: Optional[constr(min_length=1)] = None

    @field_validator("file_sizes", mode="before")
    def ensure_three_sizes(cls, v):
        if v is None:
            return [None, None, None]
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",")]
            return (parts + [None, None, None])[:3]
        if isinstance(v, list):
            parts = [p if p is not None else None for p in v]
            return (parts + [None, None, None])[:3]
        return v

    @field_validator("file_names", mode="before")
    def ensure_file_names_list(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        if isinstance(v, list):
            return [s for s in v if s]
        return v

# ------------------------
# HTTP helpers with retries
# ------------------------

def retry_decorator():
    return retry(
        reraise=True,
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.ReadTimeout, httpx.ConnectError, asyncio.TimeoutError)),
    )

@retry_decorator()
async def _get_json(client: httpx.AsyncClient, url: str) -> Dict:
    resp = await client.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

@retry_decorator()
async def _get_text(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.text

# ------------------------
# Parsers
# ------------------------

PDB_PATTERN = re.compile(r"\b([0-9][A-Za-z0-9]{3}_[A-Za-z])\b")
DOWNLOAD_SIZE_RE = re.compile(r"Download.*?\(([^)]+)\)", re.IGNORECASE)

def extract_pdb_chains(html: str) -> List[str]:
    chains = sorted(set(PDB_PATTERN.findall(html)))
    logger.info("extract_pdb_chains: found %d chains", len(chains))
    return chains

def extract_file_sizes_from_html(html: str) -> List[Optional[str]]:
    sizes = DOWNLOAD_SIZE_RE.findall(html)
    return (sizes + [None, None, None])[:3]

# ------------------------
# Fetch functions (async)
# ------------------------

async def fetch_index_html_sync() -> str:
    """Synchronous fetch used at startup for the index page."""
    def _sync():
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            r = client.get(HTML_LIST_URL, headers=HEADERS)
            r.raise_for_status()
            return r.text

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)

async def fetch_metadata_for_chain(
    client: httpx.AsyncClient, sem: Semaphore, pdb_chain: str
) -> Optional[Dict]:
    api_url = f"{API_BASE}{pdb_chain}"
    html_url = f"{BASE_URL}{pdb_chain}/{pdb_chain}.html"

    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        try:
            api_data = await _get_json(client, api_url)
        except Exception as exc:
            logger.warning("API fetch failed for %s: %s", pdb_chain, exc)
            return None

    try:
        html_text = await _get_text(client, html_url)
        sizes = extract_file_sizes_from_html(html_text)
    except Exception as exc:
        logger.warning("HTML fetch/parse failed for %s: %s", pdb_chain, exc)
        sizes = [None, None, None]

    chain_key = pdb_chain if pdb_chain in api_data else pdb_chain.upper()
    chain_data = api_data.get(chain_key, api_data if isinstance(api_data, dict) else {})

    files = chain_data.get("files") if isinstance(chain_data.get("files"), list) else None
    nb_files = len(files) if files else 3
    file_names = files if files else [
        "Analysis & MDs (only protein)",
        "MDs (only protein)",
        "MDs (total system)",
    ]

    record = {
        "source": "ATLAS",
        "source_id": chain_data.get("PDB") or pdb_chain,
        "data_set_url": api_url,
        "title": chain_data.get("protein_name") or f"ATLAS dataset for {pdb_chain}",
        "organism": chain_data.get("organism"),
        "length": int(chain_data.get("length")) if chain_data.get("length") is not None else None,
        "sequence": chain_data.get("sequence") if isinstance(chain_data.get("sequence"), str) else None,
        "crawling_date": datetime.now(timezone.utc).date().isoformat(),
        "date_creation": chain_data.get("date_creation"),
        "date_last_modification": chain_data.get("date_last_modification"),
        "nb_files": int(nb_files),
        "file_names": file_names,
        "file_sizes": sizes,
        "license": chain_data.get("license") or "CC-BY-NC 4.0",
        "authors": chain_data.get("authors") or "Yann Vander Meersche et al.",
        "doi": chain_data.get("doi") or "https://doi.org/10.1093/nar/gkad1084",
    }

    try:
        validated = DatasetRecord(**record)
        return validated.dict()
    except Exception as exc:
        logger.warning("Validation failed for %s: %s", pdb_chain, exc)
        return None

async def fetch_all(pdb_chains: List[str]) -> List[Dict]:
    sem = Semaphore(MAX_CONCURRENT_REQUESTS)
    results: List[Dict] = []
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        tasks = [fetch_metadata_for_chain(client, sem, c) for c in pdb_chains]
        for coro in asyncio.as_completed(tasks):
            try:
                res = await coro
                if res:
                    results.append(res)
            except RetryError as exc:
                logger.warning("Fetch task failed after retries: %s", exc)
            except Exception as exc:
                logger.warning("Unhandled error in fetch task: %s", exc)
    return results

# ------------------------
# Storage utilities (fixed for Pydantic types)
# ------------------------

def ensure_output_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def atomic_parquet_write(df: pd.DataFrame, path: Path) -> None:
    # write to temp dir then atomically move
    tmp_dir = tempfile.mkdtemp(dir=str(path.parent))
    tmp_file = Path(tmp_dir) / (path.name + ".tmp")
    try:
        df.to_parquet(tmp_file, index=False)
        shutil.move(str(tmp_file), str(path))
        logger.info("Wrote parquet: %s", path)
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except Exception:
            pass


def convert_pydantic_to_native(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Pydantic-specific types (HttpUrl, SequenceStr, etc.) to native Python types
    so PyArrow / pandas can write them to Parquet.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if hasattr(x, "__str__") and not isinstance(x, str) else x)
    return df


def save_results(validated_records: List[Dict], out_dir: str = OUTPUT_DIR) -> None:
    if not validated_records:
        logger.warning("No valid records to save.")
        return

    out_path = ensure_output_dir(out_dir)
    df = pd.DataFrame(validated_records)

    # ---------------- Files Parquet ----------------
    df_files = df[["source", "source_id", "nb_files", "file_names", "file_sizes"]].copy()

    # convert list columns to comma-separated strings
    df_files["file_names"] = df_files["file_names"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
    df_files["file_sizes"] = df_files["file_sizes"].apply(lambda x: ",".join([s for s in x if s]) if isinstance(x, list) else x)

    df_files = convert_pydantic_to_native(df_files)
    atomic_parquet_write(df_files, out_path / FILES_PARQUET)

    # ---------------- Metadata Parquet ----------------
    meta_cols = [
        "source", "source_id", "data_set_url", "doi", "authors", "crawling_date",
        "title", "organism", "date_creation", "date_last_modification",
        "license", "length", "sequence"
    ]
    df_meta = df[meta_cols].copy()

    df_meta = convert_pydantic_to_native(df_meta)
    atomic_parquet_write(df_meta, out_path / METADATA_PARQUET)

    logger.info("Saved all Parquet files successfully.")

# ------------------------
# Orchestration / CLI
# ------------------------

async def _run_pipeline(limit: Optional[int] = None, out_dir: str = OUTPUT_DIR) -> None:
    logger.info("Fetching index page...")
    try:
        index_html = await fetch_index_html_sync()
    except Exception as exc:
        logger.error("Failed to fetch index page: %s", exc)
        return

    chains = extract_pdb_chains(index_html)
    if limit and limit > 0:
        chains = chains[:limit]
    logger.info("Found %d chains (limit=%s)", len(chains), limit)

    logger.info("Starting async fetch of metadata...")
    results = await fetch_all(chains)
    logger.info("Fetched %d valid records", len(results))

    save_results(results, out_dir)

def main(limit: Optional[int] = None, out_dir: str = OUTPUT_DIR) -> None:
    start = time.time()
    try:
        asyncio.run(_run_pipeline(limit=limit, out_dir=out_dir))
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
    finally:
        elapsed_minutes = (time.time() - start) / 60.0
        logger.info("Done. Elapsed time: %.2f minutes", elapsed_minutes)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ATLAS metadata scraper (Pydantic-validated, strict)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of chains to fetch (0 = all)")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Output directory for parquet files")
    args = parser.parse_args()

    main(limit=args.limit or None, out_dir=args.output_dir)

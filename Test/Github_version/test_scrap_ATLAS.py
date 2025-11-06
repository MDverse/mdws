import httpx
import sqlite3
import json
import re
import asyncio
import time
import logging
from asyncio import Semaphore
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional

# ------------------------
# Configuration
# ------------------------
HTML_LIST_URL: str = "https://www.dsimb.inserm.fr/ATLAS/"
API_BASE: str = "https://www.dsimb.inserm.fr/ATLAS/api/ATLAS/metadata/"
DB_PATH: str = "catalog.db"
MDVERSE_JSON_PATH: str = "mdverse_formatted_metadata.json"

MAX_CONCURRENT_REQUESTS: int = 10
BATCH_SIZE: int = 50
REQUEST_DELAY: float = 0.2

# Configure logging for collaboration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------------
# Step 1: Scrape all PDB chains from ATLAS homepage
# ------------------------
def get_all_pdb_chains() -> List[str]:
    """Scrape the ATLAS homepage to find all PDB chain names."""
    logger.info(f"Fetching PDB chain list from: {HTML_LIST_URL}")
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(HTML_LIST_URL)
            response.raise_for_status()
            html = response.text

        pattern = re.compile(r"\b([0-9][A-Za-z0-9]{3}_[A-Za-z])\b")
        chains = sorted(set(pattern.findall(html)))
        logger.info(f"Found {len(chains)} PDB chains on ATLAS repository.")
        return chains
    except Exception as e:
        logger.error(f"Failed to fetch PDB chains: {e}")
        return []

# ------------------------
# Step 2: Initialize SQLite database
# ------------------------
def init_db() -> None:
    """Create the SQLite table if it doesn’t exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            source_id TEXT,
            data_set_url TEXT UNIQUE,
            doi TEXT,
            title TEXT,
            organism TEXT,
            crawling_date TEXT,
            date_creation TEXT,
            date_last_modification TEXT,
            nb_files TEXT,
            file_names TEXT,
            authors TEXT,
            license TEXT,
            sequence TEXT,
            length INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"SQLite database initialized at {DB_PATH}")

# ------------------------
# Step 3: Fetch metadata for one PDB chain
# ------------------------
async def fetch_api_metadata(client: httpx.AsyncClient, pdb_chain: str, sem: Semaphore) -> Optional[Dict]:
    """Fetch metadata from API for a given PDB chain."""
    api_url = f"{API_BASE}{pdb_chain}"

    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        try:
            response = await client.get(api_url)
            response.raise_for_status()
            api_data = response.json()

            chain_key = pdb_chain if pdb_chain in api_data else pdb_chain.upper()
            chain_data = api_data.get(chain_key, {})

            mdverse_entry = {
                "source": "ATLAS",
                "source_id": chain_data.get("PDB", pdb_chain),
                "data_set_url": api_url,
                "title": chain_data.get("protein_name", f"ATLAS dataset for {pdb_chain}"),
                "organism": chain_data.get("organism", "null"),
                "length": chain_data.get("length", 0),
                "sequence": chain_data.get("sequence", "null"),
                "crawling_date": datetime.now(timezone.utc).date().isoformat(),
                "date_creation": "null",
                "date_last_modification": "null",
                "nb_files": "null",
                "file_names": "null",
                "license": "CC-BY-NC 4.0",
                "authors": "Yann Vander Meersche et al.",
                "doi": "https://doi.org/10.1093/nar/gkad1084"
            }
            return mdverse_entry

        except httpx.HTTPStatusError as e:
            logger.warning(f"{pdb_chain} API returned status {e.response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching API data for {pdb_chain}: {e}")
        return None

# ------------------------
# Step 4: Fetch all metadata concurrently
# ------------------------
async def fetch_all_metadata(pdb_chains: List[str]) -> List[Dict]:
    """Fetch all metadata entries concurrently using async."""
    sem = Semaphore(MAX_CONCURRENT_REQUESTS)
    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [fetch_api_metadata(client, chain, sem) for chain in pdb_chains]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]

# ------------------------
# Step 5: Insert into SQLite + save JSON
# ------------------------
def insert_and_save_mdverse(records: List[Dict]) -> None:
    """Insert fetched metadata into SQLite and save MDverse-format JSON."""
    if not records:
        logger.warning("No records to insert.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            db_batch = [
                (r["source"], r["source_id"], r["data_set_url"], r["doi"], r["title"], r["organism"],
                 r["crawling_date"], r["date_creation"], r["date_last_modification"],
                 r["nb_files"], r["file_names"], r["authors"], r["license"],
                 r["sequence"], r["length"])
                for r in batch
            ]
            cur.executemany('''
                INSERT OR IGNORE INTO metadata (
                    source, source_id, data_set_url, doi, title, organism,
                    crawling_date, date_creation, date_last_modification,
                    nb_files, file_names, authors, license, sequence, length
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', db_batch)
            conn.commit()
            logger.info(f"Committed batch of {len(batch)} entries.")

    # Save JSON file
    Path(MDVERSE_JSON_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(MDVERSE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved MDverse-style metadata to {MDVERSE_JSON_PATH}")

# ------------------------
# Step 6: Main execution
# ------------------------
def main() -> None:
    start_time = time.time()
    init_db()

    pdb_chains = get_all_pdb_chains()
    if not pdb_chains:
        logger.warning("No PDB chains found. Exiting.")
        return

    logger.info(f"Starting async fetch for {len(pdb_chains)} API metadata entries...")
    results = asyncio.run(fetch_all_metadata(pdb_chains))

    logger.info(f"Inserting {len(results)} MDverse-style records into database and saving JSON...")
    insert_and_save_mdverse(results)

    elapsed_minutes = (time.time() - start_time) / 60
    logger.info(f"Done! Total time elapsed: {elapsed_minutes:.2f} minutes.")

if __name__ == "__main__":
    main()

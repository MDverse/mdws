"""Scrap molecular dynamics datasets and files from NOMAD.

This script scrapes molecular dynamics datasets from the Nomad repository (https://nomad-lab.eu/prod/v1/gui/search/entries).
It retrieves metadata like dataset names, descriptions, authors...etc and download links for datasets related to molecular dynamics simulations.
The scraped data is saved locally in Parquet format.

Usage :
=======
    uv run scripts/scrap_nomad.py [--log] [--out-path]

Arguments:
==========
    --log : (optional)
        Enable logging to a file.
    --out-path : (optional)
        File path to save the scraped data. Default is "../data/nomad/{timestamp}.parquet".

Example:
========
    uv run scripts/scrap_nomad.py --log --out-path ../data/nomad/nomad_metadatas.parquet
"""


# METADATA
__authors__ = ("Pierre Poulain", "Essmay Touami")
__contact__ = "pierre.poulain@u-paris.fr"
__copyright__ = "AGPL-3.0 license"
__date__ = "2025"
__version__ = "1.0.0"


# LIBRARY IMPORTS
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, ValidationError


# CONSTANTS
BASE_NOMAD_URL = "http://nomad-lab.eu/prod/v1/api/v1"
OUTPUT_DIR = "../data/nomad"


# CLASS DEFINITIONS
class NomadDataset(BaseModel):
    """Class representing a Nomad molecular dynamics dataset."""
    entry_id: str = Field(..., description="Unique identifier for the dataset entry.")
    title: str = Field(..., description="Title of the dataset.")
    doi: str = Field(..., description="Digital Object Identifier (DOI) of the dataset.")
    date_creation: datetime = Field(..., description="Creation date of the dataset.")
    date_last_modification: datetime = Field(..., description="Last modification date of the dataset.")
    nb_files: int = Field(..., description="Number of files in the dataset.")
    file_names: List[str] = Field(..., description="List of file names in the dataset.")
    authors: List[str] = Field(..., description="List of authors associated with the dataset.")
    license: str = Field(..., description="License under which the dataset is shared.")
    description: str = Field(..., description="Description or comment about the dataset.")
    file_analyses: List[Dict[str, Any]] = Field(..., description="Analysis results related to the files.")


# FUNCTIONS
def parse_arguments() -> Tuple[bool, str, str]:
    """ Parse command line arguments.

    Returns:
    --------
    log : bool
        Whether to enable logging to a file.
    out_path : str
        The output file path for the scraped data.
    """
    parser = argparse.ArgumentParser(
        description="Scrape molecular dynamics datasets from NOMAD."
    )
    parser.add_argument(
        "--log",
        action="store_true",
        default=False,
        help="Enable logging to a file.",
    )
    parser.add_argument(
        "--out-path",
        type=str,
        default=f"{OUTPUT_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet",
        help="Output file path for the scraped data.",
    )

    args = parser.parse_args()
    # retrieve output directory
    folder_out_path = os.path.dirname(args.out_path)
    file_name = os.path.basename(args.out_path)

    return args.log, folder_out_path, file_name


def test_nomad_connection() -> bool:
    """ Test connection to the NOMAD API.

    Returns:
    --------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to NOMAD API...")
    try:
        r = httpx.get(f"{BASE_NOMAD_URL}/entries", timeout=5)
        if r.status_code == 200:
            logger.success("Connected to NOMAD API successfully !")
            return True
    except httpx.RequestException:
        logger.error("Failed to connect to NOMAD API.")
        return False


def fetch_entries_md_related() -> Tuple[List[Dict[str, Any]], str]:
    """
    Fetch all Molecular Dynamics (MD)-related entries from the NOMAD API.

    Returns
    -------
    Tuple[List[Dict[str, Any]], str]:
        - A list of entries related to Molecular Dynamics workflows (JSON objects).
        Returns an empty list if the request fails.
        - The current timestamp in ISO 8601 format (e.g., '2023-03-05T22:01:12').

    """
    logger.debug("Fetching Molecular Dynamics related entries from NOMAD API... (usually take less than 3 minutes!)")
    # Current timestamp in ISO format
    fetch_time: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        # Build the request URL with a query filtering for 'MolecularDynamics' workflow
        url = (
            f"{BASE_NOMAD_URL}/entries/export"
            "?owner=public"
            "&json_query=%7B%22results.method.workflow_name%22%3A%22MolecularDynamics%22%7D"
        )

        # Perform the HTTP GET request with a long timeout to accommodate large data (usually take less than 3 minutes)
        response = httpx.get(url, timeout=1000)
        response.raise_for_status()

        # Parse JSON data
        entries_md = response.json()
        logger.success(f"Fetched {len(entries_md)} MD-related entries from NOMAD successfully ! \n")
        return entries_md, fetch_time
    
    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], fetch_time
    

def parse_entry_metadata(data: Dict[str, Any], fetch_time: str) -> Dict[str, Any]:
    """
    Parse relevant metadata fields from a single NOMAD entry JSON.

    Parameters
    ----------
    data : Dict[str, Any]
        JSON response for a single NOMAD entry.
    fetch_time : str
        Timestamp when the data was fetched.

    Returns
    -------
    Dict[str, Any]
        Flattened metadata dictionary for one entry.
    """
    entry_id = data.get("entry_id")
    dataset = data.get("datasets", [{}])[0] if data.get("datasets") else {}

    return {
        "source": "NOMAD",
        "source_id": f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}",
        "doi": data.get("references"),
        "title": dataset.get("dataset_name"),
        "date_creation": dataset.get("dataset_create_time"),
        "date_last_modification": dataset.get("dataset_modified_time"),
        "date_last_crawled": fetch_time,
        "nb_files": len(data.get("files", [])),
        "file_names": data.get("files", []),
        "authors": [a.get("name") for a in data.get("authors", [])],
        "license": data.get("license"),
        "description": data.get("comment"),
        "file_analyses": data.get("results"),
    }


def parse_nomad_dataset_parallel(nomad_data: List[Dict[str, Any]], fetch_time: str, max_workers: Optional[int] = None) -> pd.DataFrame:
    """
    Parse all NOMAD entries in parallel and return a combined DataFrame.

    Parameters
    ----------
    nomad_data : List[Dict[str, Any]]
        List of NOMAD entry JSON objects.
    fetch_time : str
        Timestamp when data was fetched.
    max_workers : int, optional
        Maximum number of threads to use for parallel parsing (default is None, which uses os.cpu_count()).

    Returns
    -------
    pd.DataFrame
        DataFrame containing parsed metadata for all entries.
    """
    results = []
    # Get all available CPU cores if max_workers is not specified
    if max_workers is None:
        max_workers = os.cpu_count()
    logger.debug(f"Parsing NOMAD dataset in parallel with {max_workers} workers...")

    # Start parallel parsing
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(parse_entry_metadata, entry, fetch_time): entry for entry in nomad_data}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Error parsing {futures[future]}: {e}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.success(f"Parsed NOMAD entries in {elapsed_time:.2f} seconds successfully ! \n")
    return pd.DataFrame(results)


def save_nomad_metadata_to_parquet(df: pd.DataFrame, output_path: str) -> str:
    """
    Save parsed NOMAD metadata DataFrame to a local file.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing parsed NOMAD metadata.
    output_path : str


    Returns
    -------
    str
        Path to the saved file.
    """
    logger.debug("Saving NOMAD metadata to a Parquet file...")
    df.to_parquet(output_path, index=False)
    logger.success(f"NOMAD metadata saved to: {output_path} successfully! \n")
    return output_path


def scrap_nomad_data(folder_out_path: str, file_name: str) -> None:
    """ Scrap molecular dynamics datasets and files from NOMAD.
    
    Parameters:
    -----------
    folder_out_path : str
        The output folder path for the scraped data.
    file_name : str
        The output file name for the scraped data."""
    logger.info("Starting Nomad data scraping...")

    if test_nomad_connection:
        # Define output directory
        os.makedirs(folder_out_path, exist_ok=True)
        output_file_path = os.path.join(folder_out_path, file_name)

        # Fetch NOMAD entries metadata
        nomad_data, fetch_time = fetch_entries_md_related()
        if nomad_data == []:
            logger.warning("No data fetched from NOMAD.")
            return
        # Parse NOMAD entries metadata in parallel
        nomad_metadata_df = parse_nomad_dataset_parallel(nomad_data, fetch_time)

        # Save parsed metadata to local file
        save_nomad_metadata_to_parquet(nomad_metadata_df, output_file_path)
    
        #logger.success(f"Scrapped NOMAD data successfully and saved to {output_file_path} !")

    else:
        logger.error("Cannot scrap data, no connection to NOMAD API.")
        sys.exit()


if __name__ == "__main__":
    start_time = time.time()

    # Parse arguments
    log, folder_out_path, file_name = parse_arguments()
    # Configure logging
    if log:   
        logger.add("../logs/scrap_nomad_{time:YYYY-MM-DD}.log")

    # Create output repe
    
    # Scrap NOMAD data
    scrap_nomad_data(folder_out_path, file_name)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.success(f"Completed Nomad data scraping in {elapsed_time:.2f} seconds successfully 🎉")
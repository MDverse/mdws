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
from tqdm import tqdm
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
JSON_PAYLOAD_DATASETS = {
  "owner": "visible",
  "query": {
    "results.method.workflow_name:any": [
      "MolecularDynamics"
    ]
  },
  "aggregations": {},
  "pagination": {
    "order_by": "upload_create_time",
    "order": "desc",
    "page_size": None
  },
  "required": {
    "exclude": [
      "quantities",
      "sections",
    ]
  }
}

# CLASS DEFINITIONS
class NomadDataset(BaseModel):
    """Class representing a Nomad molecular dynamics dataset."""

    entry_id: str = Field(..., description="Unique identifier for the dataset entry.")
    title: str = Field(..., description="Title of the dataset.")
    date_created: datetime = Field(..., description="Creation date of the dataset.", alias="date_created")
    date_last_modification: datetime = Field(..., description="Last modification date of the dataset.", alias="date_last_updated")
    nb_files: int = Field(..., description="Number of files in the dataset.")
    file_names: List[str] = Field(..., description="List of file names in the dataset.")
    authors: List[str] = Field(..., description="List of authors associated with the dataset.")
    license: str = Field(..., description="License under which the dataset is shared.")
    description: str = Field(..., description="Description or comment about the dataset.")
    file_analyses: List[Dict[str, Any]] = Field(..., description="Analysis results related to the files.")


# FUNCTIONS
def parse_arguments() -> Tuple[bool, str, str]:
    """Parse command line arguments.

    Returns:
    --------
    log : bool
        Whether to enable logging to a file.
    out_path : str
        The output file path for the scraped data.
    """
    logger.info("Starting to parse command-line arguments...")
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

    logger.debug(f"Logger: '{args.log}'")
    logger.debug(f"Output folder path: '{folder_out_path}'")
    logger.debug(f"Output file name: '{file_name}'")

    logger.success("Parsed arguments sucessfully!\n")
    return args.log, folder_out_path, file_name


def test_nomad_connection() -> bool:
    """Test connection to the NOMAD API.

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


def fetch_entries_md_related_by_batch(page_size: int = 50) -> List[Tuple[List[Dict[str, Any]], str]]:
    """
    Fetch all Molecular Dynamics (MD)-related entries from the NOMAD API with pagination.

    Parameters
    ----------
    page_size : int
        Number of entries to fetch per page.

    Returns
    -------
    List[Tuple[List[Dict[str, Any]], str]]:
        - A list of tuples (entries_list, fetch_time) for each batch.
    """
    logger.info("Fetching Molecular Dynamics related entries from NOMAD API by batch...")
    fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    all_entries_with_time = []
    next_page_value = None
    total_entries = None

    # Fetch the first page
    try:
        logger.debug("Requesting first page...")
        fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # HTTP request
        response = httpx.post(
            f"{BASE_NOMAD_URL}/entries/query",
            json={**JSON_PAYLOAD_DATASETS,
              "pagination": {**JSON_PAYLOAD_DATASETS["pagination"], "page_size": page_size}},
            timeout=100)
        response.raise_for_status()
        # Get the formated response with request metadatas in JSON format
        first_entries_with_request_md = response.json()
        # Get the total entries from the request md
        total_entries = first_entries_with_request_md["pagination"]["total"]
        # Get the ID to start the next batch of entries
        next_page_value = first_entries_with_request_md["pagination"]["next_page_after_value"]
        # Get only the entries metadatas
        first_entries = first_entries_with_request_md["data"]
        # Add it with the crawled time
        all_entries_with_time.append((first_entries, fetch_time))
        logger.debug(f"Fetched first {len(first_entries_with_request_md['data'])}/{total_entries} entries")

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], None

    # Paginate through remaining entries
    logger.debug("Paginate through remaining entries... (usually takes around 3 minutes)")
    with tqdm(
        total=total_entries,
        desc="Fetching MD entries from NOMAD",
        colour="blue",
        ncols=100,
        ascii="░▒█",
        unit="entry",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    ) as pbar:
        # Initial update for the first batch already fetched
        pbar.update(len(first_entries))
        while sum(len(batch[0]) for batch in all_entries_with_time) < total_entries and next_page_value:
            try:
                fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                # HTTP request
                response = httpx.post(
                    f"{BASE_NOMAD_URL}/entries/query",
                    json={**JSON_PAYLOAD_DATASETS,
                    "pagination": {**JSON_PAYLOAD_DATASETS["pagination"],
                    "page_size": page_size,
                    "next_page_after_value": next_page_value}},
                    timeout=100)
                response.raise_for_status()
                next_batch = response.json()
                all_entries_with_time.append((next_batch["data"], fetch_time))

                # Update the bar progression
                pbar.update(len(next_batch["data"]))
                # Update the next entry to begin with
                next_page_value = next_batch.get("pagination", {}).get("next_page_after_value", None)

            except httpx.HTTPError as e:
                logger.error(f"HTTP error occurred while fetching next page: {e}")
                break

    logger.success(f"Fetched {sum(len(batch[0]) for batch in all_entries_with_time)} Molecular Dynamics entries from NOMAD successfully !")
    return all_entries_with_time


def fetch_entries_md_related_once() -> Tuple[List[Dict[str, Any]], str]:
    # NOTE: Deprecated -> single-request fetch can be too heavy; prefer batch fetching
    """
    Fetch all Molecular Dynamics (MD)-related entries from the NOMAD API in a single request.
    Note:
        This function is not recommended for regular use, as the server must prepare
        approximately 1.5 GB of data in one batch, which can be slow or memory-intensive.

    Returns
    -------
    Tuple[List[Dict[str, Any]], str]:
        - A list of entries related to Molecular Dynamics workflows (JSON objects).
        Returns an empty list if the request fails.
        - The current timestamp in ISO 8601 format (e.g., '2023-03-05T22:01:12').

    """
    logger.debug(
        "Fetching Molecular Dynamics related entries from NOMAD API... (usually take less than 3 minutes!)"
    )
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
        logger.success(
            f"Fetched {len(entries_md)} MD-related entries from NOMAD successfully ! \n"
        )
        return entries_md, fetch_time

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], fetch_time


def parse_and_validate_entry_metadatas(nomad_data: List[Tuple[List[Dict[str, Any]], str]]) -> List[NomadDataset]:
    """
    Parse and validate metadata fields for all NOMAD entries in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of entries and the fetch_time.

    Returns
    -------
    List[NomadDataset]
        List of validated metadata in NomadDataset format for all entries.
    """
    validated_entries = []

    for entries_list, fetch_time in nomad_data:
        for data in entries_list:
            entry_id = data.get("entry_id")
            dataset = data.get("datasets", [{}])[0] if data.get("datasets") else {}

            parsed_entry = {
                "source": "NOMAD",
                "source_id": entry_id,
                "url": f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}",
                "links": data.get("references"),
                "title": dataset.get("entry_name"),
                "date_created": dataset.get("entry_create_time"),
                "date_last_updated": dataset.get("last_processing_time"),
                "date_last_crawled": fetch_time,
                "nb_files": len(data.get("files", [])),
                "file_names": data.get("files", []),
                "author_names": [a.get("name") for a in data.get("authors", [])],
                "author_ids": [a.get("user_id") for a in data.get("authors", [])],
                "license": data.get("license"),
                "description": data.get("comment"),
                "simulation_program": (
                    data.get("results", {})
                        .get("method", {})
                        .get("simulation", {})
                        .get("program_name")
                ),
                "simulation_program_version": (
                    data.get("results", {})
                        .get("method", {})
                        .get("simulation", {})
                        .get("program_version")
                )
            }

            try:
                # Validate and normalize data collected wieh pydantic model
                dataset_model = NomadDataset(**parsed_entry)
                validated_entries.append(dataset_model)
            except ValidationError as e:
                logger.error(f"Validation failed for entry {entry_id}: {e}")

    return validated_entries


def parse_nomad_files(batch_json: Dict[str, Any], fetch_time: str) -> List[Dict[str, Any]]:
    """
    Extract file metadata from a NOMAD batch JSON.

    Args:
        batch_json (Dict[str, Any]): JSON object returned by NOMAD API for a page of entries.
        fetch_time (str): Timestamp when the files were fetched.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing file metadata.
    """
    for entry in batch_json.get("data", []):
        entry_id = entry.get("entry_id")
        for file in entry.get("files", []):
            name_file = file["path"].split("/")[-1]
            file_extension = name_file.split(".")[-1]
            file_path = (
                f"https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/"
                f"{entry_id}/files/{name_file}"
            )
            size = file.get("size", None)

    return {
            "entry_id": entry_id,
            "name_file": name_file,
            "type": file_extension,
            "size": size,
            "file_path": file_path,
            "date_last_crawled": fetch_time
            }


def fetch_files_metadata(page_size: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch file metadata for NOMAD Molecular Dynamics entries.

    Parameters
    ----------
    page_size : int
        Number of entries to fetch per page.

    Returns:
    --------
    List[Dict[str, Any]]: A list of dictionaries containing file metadata.
        Each dictionary has the following structure:
        {
            "entry_id": str,
            "name_file": str,
            "size": int,
            "file_path": str
        }
    """
    logger.info("Fetching files metadata from NOMAD...")
    fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    files_metas = []

    # Fetch the first page
    try:
        url = (
            f"{BASE_NOMAD_URL}/entries/rawdir"
            "?owner=public"
            f"&page_size={page_size}"
            "&order=asc"
            "&json_query=%7B%22results.method.workflow_name%22%3A%22MolecularDynamics%22%7D"
        )
        logger.debug(f"Requesting first page: {url}")
        response = httpx.get(url, timeout=1000)
        response.raise_for_status()

        first_files_metas = response.json()
        files_metas.append(parse_nomad_files(first_files_metas, fetch_time))

        total_entries = first_files_metas["pagination"]["total"]
        next_page_value = first_files_metas["pagination"]["next_page_after_value"]
        logger.debug(
            f"Fetched metadata for the first {len(first_files_metas['data'])} entries "
            f"({len(files_metas)} files out of {total_entries} entries)"
        )

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return []

    # Paginate through remaining entries
    with tqdm(
        desc="Fetching NOMAD Molecular Dynamics files metadatas",
        colour="blue",
        ncols=100,
        ascii="░▒█",
        unit="file",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    ) as pbar:
        # Initial update for the first batch already fetched
        pbar.update(len(files_metas))
        while next_page_value:
            try:
                url = (
                    f"{BASE_NOMAD_URL}/entries/rawdir"
                    "?owner=public"
                    f"&page_size={page_size}"
                    "&order=asc"
                    "&json_query=%7B%22results.method.workflow_name%22%3A%22MolecularDynamics%22%7D"
                    f"&page_after_value={next_page_value}"
                )
                response = httpx.get(url, timeout=1000)
                response.raise_for_status()

                next_batch = response.json()
                files_metas.append(parse_nomad_files(next_batch, fetch_time))
                pbar.update(len(files_metas))

                # Update the next entry to begin with
                next_page_value = next_batch["pagination"]["next_page_after_value"]

            except httpx.HTTPError as e:
                logger.error(f"HTTP error occurred while fetching next page: {e}")
                break

    unique_entry_ids = {f["entry_id"] for f in files_metas}
    logger.success(
        f"Fetched {len(files_metas)} file metadata entries for {len(unique_entry_ids)} NOMAD entries successfully!"
    )
    return files_metas


def save_entries_metadata_to_parquet(df: pd.DataFrame, output_path: str) -> str:
    """
    Save parsed NOMAD entries metadata DataFrame to a local file.

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


def save_files_metadata_to_parquet(df: pd.DataFrame, output_path: str) -> str:
    """
    Save parsed NOMAD files metadata DataFrame to a local file.

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
    logger.debug("Saving NOMAD files metadata to a Parquet file...")
    df.to_parquet(output_path, index=False)
    logger.success(f"NOMAD files metadata saved to: {output_path} successfully! \n")
    return output_path


def scrap_nomad_data(folder_out_path: str, file_name: str) -> None:
    """Scrap molecular dynamics datasets and files from NOMAD.

    Parameters:
    -----------
    folder_out_path : str
        The output folder path for the scraped data.
    file_name : str
        The output file name for the scraped data."""
    logger.info("Starting Nomad data scraping...")
    start_time = time.time()

    if test_nomad_connection:
        # Fetch NOMAD entries metadata
        nomad_data = fetch_entries_md_related_by_batch()
        if nomad_data == []:
            logger.warning("No data fetched from NOMAD.")
            return

        # Parse and validate NOMAD entry metadatas with a pydantic model (NomadDataset)
        nomad_metadatas_validated = parse_and_validate_entry_metadatas(nomad_data)

        # Save parsed metadata to local file
        # save_entries_metadata_to_parquet(nomad_metadatas_validated, output_file_path)

        # Parse NOMAD file metadatas
        # files_metas = fetch_files_metadata
        # df = pd.DataFrame(files_metas)
        # output_file_path = os.path.join(folder_out_path, "nomad_file_metadatas.parquet")
        # save_files_metadata_to_parquet(df, output_file_path)

        # end_time = time.time()
        # elapsed_time = end_time - start_time
        # logger.success(
        #     f"Completed Nomad data scraping in {elapsed_time:.2f} seconds successfully 🎉"
        # )

    else:
        logger.error("Cannot scrap data, no connection to NOMAD API.")
        sys.exit()


if __name__ == "__main__":
    # Parse arguments
    log, folder_out_path, file_name = parse_arguments()
    # Configure logging
    if log:
        log_folder = Path("logs")
        log_folder.mkdir(parents=True, exist_ok=True)
        logger.add(log_folder / "scrap_nomad_{time:YYYY-MM-DD}.log")

    # Scrap NOMAD data
    scrap_nomad_data(folder_out_path, file_name)



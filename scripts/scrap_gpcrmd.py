"""
Scrape datasets and files from GPCRMD.

This script fetches datasets from the GPCRMD repository (https://www.gpcrmd.org/).
It collects metadata such as dataset names, descriptions, authors, download links,
and other relevant information for all available datasets.
Additionally, it retrieves file metadata for each dataset, including file paths
in GPCRMD, file size, type/extension, etc.

The scraped data is validated against Pydantic models (`BaseDataset` and `BaseFile`)
and saved locally in Parquet format:
- "data/gpcrmd/{timestamp}/validated_entries.parquet"
- "data/gpcrmd/{timestamp}/validated_files.parquet"

Entries that fail validation are saved as:
- "data/gpcrmd/{timestamp}/unvalidated_entries.parquet"
- "data/gpcrmd/{timestamp}/unvalidated_files.parquet"


Usage:
======
    uv run -m scripts.scrap_gpcrmd [--out-path]

Arguments:
==========
    --out-path : (optional)
        Folder path to save the scraped GPCRMD data (dataset and file metadata).
        Default is "data/gpcrmd/{timestamp}".

Example:
========
   uv run -m scripts.scrap_gpcrmd

This command will:
    1. Fetch all available datasets from GPCRMD in batches.
    2. Parse their metadata and validate them using the Pydantic models `BaseDataset`
       and `BaseFile`.
    3. Save both the validated and unvalidated dataset entries to
       "data/gpcrmd/{timestamp}/{validated or unvalidated}_entries.parquet".
    4. Save file metadata similarly for validated and unvalidated files.
"""

# METADATAS
__authors__ = ("Pierre Poulain", "Essmay Touami")
__contact__ = "pierre.poulain@u-paris.fr"
__copyright__ = "AGPL-3.0 license"
__date__ = "2025"
__version__ = "1.0.0"


# LIBRARY IMPORTS
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import ValidationError
from tqdm import tqdm

from models.dataset_model import BaseDataset
from models.file_model import BaseFile

# CONSTANTS
BASE_GPCRMD_URL = "https://www.gpcrmd.org/api/search_all/info/"


# FUNCTIONS
def setup_logger(loguru_logger: Any, log_dir: str | Path = "logs") -> None:
    """Configure a Loguru logger to write logs into a rotating daily log file.

    Parameters
    ----------
    loguru_logger : Any
        A Loguru logger instance (typically `loguru.logger`).
    log_dir : str or Path, optional
        Directory where log files will be stored. Default is "logs".
    """
    # Ensure log directory exists
    log_folder = Path(log_dir)
    log_folder.mkdir(parents=True, exist_ok=True)
    # Reset any previous configuration
    loguru_logger.remove()
    # Define log format
    fmt = (
        "{time:YYYY-MM-DD HH:mm:ss}"
        "| <level>{level:<8}</level> "
        "| <level>{message}</level>"
    )
    loguru_logger.add(
        log_folder / "scrap_gpcrmd_data_{time:YYYY-MM-DD}.log",
        format=fmt,
        level="DEBUG",
    )
    loguru_logger.add(
        sys.stdout,
        format=fmt,
        level="DEBUG",
    )


def fetch_entries_once() -> tuple[list[dict[str, Any]], str]:
    """
    Fetch all entries from the GPCRMD API.

    Returns
    -------
    Tuple[List[Dict[str, Any]], str]:
        - A list of entries (JSON objects).
        Returns an empty list if the request fails.
        - The current timestamp in ISO 8601 format (e.g., '2023-03-05T22:01:12').
    """
    logger.debug(
        "Fetching entries from GPCRMD API... (usually take \
        less than 1 minutes!)"
    )
    # Current timestamp in ISO format
    fetch_time: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        # Perform the HTTP GET request with a long timeout to accommodate large data
        response = httpx.get(BASE_GPCRMD_URL, timeout=1000)
        response.raise_for_status()

        # Parse JSON data
        entries_md = response.json()
        logger.success(
            f"Fetched {len(entries_md)} MD-related entries from GPCRMD successfully! \n"
        )
        return entries_md, fetch_time

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], fetch_time


def retrieve_metadata(url: str, field_name: str, timeout: int = 10) -> str | None:
    """
    Retrieve a specific metadata field from a webpage.

    Parameters
    ----------
    url : str
        The URL of the webpage to fetch.
    field_name : str
        The name of the metadata field to extract (case-sensitive).
    timeout : int, optional
        Timeout in seconds for the HTTP request (default is 10).

    Returns
    -------
    str | None
        The value of the metadata field if found, otherwise None.

    """
    # Try to send an HTTP GET request to the given URL of the dataset.
    try:
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()
    except httpx.RequestError as e:
        logger.warning(f"Failed to fetch {field_name} from {url}: {e}")
        return None
    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")
    bold_tag = soup.find("b", string=lambda t: t and t.strip() == field_name)
    if not bold_tag:
        return None
    # Get all the text from the parent element of the <b> tag
    parent_text = bold_tag.parent.get_text(strip=True)
    if ":" not in parent_text:
        return None
    # Get only what is after the "field_name:"
    return parent_text.split(":", 1)[1].strip() or None


def retrieve_reference_links(url: str, timeout: int = 10) -> list[str] | None:
    """
    Retrieve reference URLs from the References section of a GPCRMD entry page.

    Parameters
    ----------
    url : str
        The URL of the GPCRMD entry page.
    timeout : int, optional
        Timeout in seconds for the HTTP request (default is 10).

    Returns
    -------
    list[str] | None
        List of reference URLs (starting with http:// or https://) if found,
        otherwise None.
    """
    try:
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()
    except httpx.RequestError as e:
        logger.warning(f"Failed to fetch reference links from {url}: {e}")
        return None
    # Parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")
    # Find the <h3> header with text "References"
    header = soup.find("h3", string=lambda t: t and t.strip() == "References")
    if not header:
        return None
    # Get the corresponding content div containing the links
    content_div = header.find_next_sibling("div", class_="techinfo_content")
    if not content_div:
        return None

    links: list[str] = []
    # Collect all hrefs that start with http:// or https://
    for a in content_div.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("http://", "https://")):
            links.append(href)

    return links or None


def count_simulation_files(url: str, timeout: int = 10) -> int | None:
    """
    Count files in the dataset webpage.

    Especially in 'Simulation output files' and 'Simulation protocol \
    & starting files' sections.

    Returns
    -------
    int | None
        The number of files related to this dataset.
    """
    try:
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()
    except httpx.RequestError as e:
        logger.warning(f"Failed to fetch file counts from {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Helper function to count unique links in a container div
    def count_links(container_id: str) -> int:
        container = soup.find("div", id=container_id)
        if not container:
            return 0

        # Collect all hrefs and remove duplicates while preserving order
        links = ([a["href"].strip() for a in container.find_all("a", href=True)
            if a["href"].strip()]
        )
        return len(dict.fromkeys(links))

    output_files_count = count_links("allfiles")
    protocol_files_count = count_links("paramfiles")

    return output_files_count + protocol_files_count


def parse_and_validate_entry_metadatas(
    entries_list: list[dict],
    fetch_time: str
) -> tuple[list[BaseDataset], list[dict]]:
    """
    Parse and validate metadata fields for a list of GPCRMD entries.

    Parameters
    ----------
    entries_list : list of dict
        List of dictionaries, each representing the metadata of a GPCRMD entry.
    fetch_time : str
        Timestamp (as a string) indicating when the data was fetched.

    Returns
    -------
    Tuple[List[BaseDataset], List[Dict]]
        - List of successfully validated `BaseDataset` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validation of GPCRMD entries...")
    validated_entries = []
    non_validated_entry_ids = []
    total_entries = len(entries_list)

    for data in tqdm(entries_list):
        entry_id = str(data.get("dyn_id"))

        # Extract molecules and number total of atoms if available
        total_atoms = data.get("atom_num")
        dyncomp = data.get("dyncomp", [])
        molecules = [comp.get("resname") for comp in dyncomp if comp.get("resname")]
        url = data.get("url")
        author_names = [retrieve_metadata(url, "Submitted by")]
        description = retrieve_metadata(url, "Description")
        stime = retrieve_metadata(url, "Accumulated simulation time")
        refs = retrieve_reference_links(url)
        nb_files = count_simulation_files(url)

        parsed_entry = {
            "dataset_repository": "GPCRMD",
            "dataset_project": "GPCRMD",
            "dataset_id_in_repository": entry_id,
            "dataset_id_in_project": entry_id,
            "dataset_url_in_repository": url,
            "dataset_url_in_project": url,
            "links": refs,
            "title": data.get("modelname"),
            "date_created": data.get("creation_timestamp"),
            "date_last_fetched": fetch_time,
            "nb_files": nb_files,
            "author_names": author_names,
            "description": description,
            "simulation_program_name": data.get("mysoftware"),
            "simulation_program_version": data.get("software_version"),
            "nb_atoms": total_atoms,
            "molecule_names": molecules,
            "forcefield_model_name": data.get("forcefield"),
            "forcefield_model_version": data.get("forcefield_version"),
            "timestep": data.get("timestep"),
            "delta": data.get("delta"),
            "simulation_time": stime
            }
        try:
            # Validate and normalize data collected wieh pydantic model
            dataset_model = BaseDataset(**parsed_entry)
            validated_entries.append(dataset_model)
        except ValidationError as e:
            logger.error(f"Validation failed for entry {entry_id}")
            for err in e.errors():
                logger.error(f"  Field: {'.'.join(str(x) for x in err['loc'])}")
                logger.error(f"  Error: {err['msg']} (type={err['type']})")
            non_validated_entry_ids.append(parsed_entry)

    logger.success(
        f"Parsing completed: {len(validated_entries)} validated / {total_entries} total\
            entries successfully! \n"
    )
    return validated_entries, non_validated_entry_ids


def parse_and_validate_files_metadatas(
    entries_list: list[dict],
    fetch_time: str
) -> tuple[list[BaseFile], list[dict]]:
    """
    Parse and validate metadata for GPCRMD files.

    Parameters
    ----------
    entries_list : list[dict]
        List of file entries, each containing metadata such as 'dyn_id' and 'url'.
    fetch_time : str
        Timestamp indicating when the data was fetched.

    Returns
    -------
    tuple[list[BaseFile], list[dict]]
        - List of validated `BaseFile` objects.
        - List of file entries that failed validation.
    """
    logger.info("Starting parsing and validation of GPCRMD files...")
    validated_entries: list[BaseFile] = []
    non_validated_entry_ids: list[dict] = []
    total_files = len(entries_list)

    # Loop over the first two entries for demonstration
    for data in tqdm(entries_list):
        entry_id = str(data.get("dyn_id"))
        url = data.get("url")

        # Fetch the file page
        try:
            response = httpx.get(url, timeout=10)
            response.raise_for_status()
        except httpx.RequestError as e:
            logger.warning(f"Failed to fetch file page for {entry_id}: {e}")
            non_validated_entry_ids.append(data)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        sections = ["allfiles", "paramfiles"]

        # Iterate over sections containing files
        for sec_id in sections:
            container = soup.find("div", id=sec_id)
            if not container:
                continue

            # Process each file link
            for a in container.find_all("a", href=True):
                file_path = f"https://www.gpcrmd.org/{a['href'].strip()}"
                if not file_path:
                    continue

                file_name = os.path.basename(file_path)
                file_extension = os.path.splitext(file_name)[1].lstrip(".").lower()

                # Try to fetch file size via HEAD request
                size: int = None
                try:
                    head_resp = httpx.head(file_path, timeout=10, follow_redirects=True)
                    size = int(head_resp.headers.get("Content-Length", 0))
                except httpx.RequestError as e:
                    logger.warning(f"Failed to fetch file size for {file_name}: {e}")

                parsed_entry = {
                    "dataset_repository": "GPCRMD",
                    "dataset_id_in_repository": entry_id,
                    "file_name": file_name,
                    "file_type": file_extension,
                    "file_size": size,
                    "file_url_in_repository": file_path,
                    "date_last_fetched": fetch_time,
                }

                # Validate and normalize entry using Pydantic model
                try:
                    dataset_model = BaseFile(**parsed_entry)
                    validated_entries.append(dataset_model)
                except ValidationError as e:
                    logger.error(f"Validation failed for file {entry_id}: {e}")
                    non_validated_entry_ids.append(parsed_entry)

    logger.success(
        f"Parsing completed: {len(validated_entries)} validated / {total_files} \
            total files successfully!"
    )
    return validated_entries, non_validated_entry_ids


def save_metadatas_to_parquet(
    folder_out_path: Path,
    metadatas_validated: list[BaseDataset] | list[BaseFile],
    metadatas_unvalidated: list[dict],
    tag: str,
) -> None:
    """
    Save GPCRMD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    folder_out_path : Path
        Folder path where Parquet files will be saved.
    metadatas_validated : List[BaseDataset]
        List of validated entries.
    metadatas_unvalidated : List[Dict]
        List of unvalidated entries as dictionaries.
    tag: str
        Tag to know if its entries or files metadata to save.
    """
    logger.info("Saving GPCRMD entries metadatas to a Parquet file...")
    # Ensure output folder exists
    Path(folder_out_path).mkdir(parents=True, exist_ok=True)

    # Save validated entries
    if tag == "entries":
        validated_path = os.path.join(folder_out_path, "validated_entries.parquet")
    elif tag == "files":
        validated_path = os.path.join(folder_out_path, "validated_files.parquet")
    try:
        # Convert list of Pydantic models to list of dicts
        validated_dicts = [entry.model_dump() for entry in metadatas_validated]
        df_validated = pd.DataFrame(validated_dicts)
        df_validated.to_parquet(validated_path, index=False)
        logger.success(
            f"GPCRMD validated metadatas saved to: {validated_path} successfully!"
        )
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save validated metadata to {validated_path}: {e}")

    # Save unvalidated entries
    if tag == "entries":
        unvalidated_path = os.path.join(
            folder_out_path, "unvalidated_entries.parquet"
        )
    elif tag == "files":
        unvalidated_path = os.path.join(
            folder_out_path, "unvalidated_files.parquet"
        )
    try:
        if len(metadatas_unvalidated) != 0:
            df_unvalidated = pd.DataFrame(metadatas_unvalidated)
            df_unvalidated.to_parquet(unvalidated_path, index=False)
            logger.success(
            f"GPCRMD unvalidated metadatas saved to: {unvalidated_path} successfully!"
            )
        else:
            logger.warning("There is no unvalidated entries to save!")
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save unvalidated metadata to {unvalidated_path}: {e}")


@click.command()
@click.option(
    "--out-path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    default=Path(f"data/gpcrmd/{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
    show_default=True,
    help="Folder path to save the scraped GPCRMD data (Dataset and File metadatas)"
)
def scrap_gpcrmd_data(out_path: Path) -> None:
    """Scrap datasets and files from GPCRMD.

    Parameters
    ----------
    out_path : Path
        The output folder path for the scraped data.
    """
    setup_logger(logger, out_path)
    logger.info("Starting GPCRMD data scraping...")
    start_time = time.time()

    # Fetch entries metadata
    entries, fetch_time = fetch_entries_once()
    if entries == []:
        logger.warning("No data fetched from GPCRMD.")
        return
    # Parse and validate  entry metadatas with a pydantic model (BaseDataset)
    entries_validated, entries_unvalidated = (
        parse_and_validate_entry_metadatas(entries, fetch_time)
    )
    # Save parsed metadata to local file
    save_metadatas_to_parquet(
        out_path,
        entries_validated,
        entries_unvalidated,
        tag="entries"
    )

    # Fetch, parse and validate the file metadatas with a pydantic model (BaseFile)
    files_metadata_validated, files_metadata_unvalidated = (
        parse_and_validate_files_metadatas(entries, fetch_time)
    )
    save_metadatas_to_parquet(
        out_path,
        files_metadata_validated,
        files_metadata_unvalidated,
        tag="files"
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    logger.success(
        f"Completed GPCRMD data scraping in {hours} h {minutes} min {seconds} sec 🎉"
    )


if __name__ == "__main__":
    # Scrap GPCRMD data
    scrap_gpcrmd_data()

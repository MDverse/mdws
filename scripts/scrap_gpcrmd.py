"""
Scrape datasets and files from GPCRMD.

This script fetches datasets from the GPCRMD repository (https://www.gpcrmd.org/).
It collects metadata such as dataset names, descriptions, authors, download links,
and other relevant information for all available datasets.
Additionally, it retrieves file metadata for each dataset, including file paths
in GPCRMD, file size, type/extension, etc.

The scraped data is validated against Pydantic models (`DatasetModel` and `File Model`)
and saved locally in Parquet format:
- "data/gpcrmd/{timestamp}/gpcrmd_datasets.parquet"
- "data/gpcrmd/{timestamp}/gpcrmd_files.parquet"

Entries that fail validation are saved as:
- "data/gpcrmd/{timestamp}/not_validated_gpcrmd_datasets.parquet"
- "data/gpcrmd/{timestamp}/not_validated_gpcrmd_files.parquet"


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
    1. Fetch all available datasets from GPCRMD.
    2. Parse their metadata and validate them using the Pydantic models `DatasetModel`
       and `File Model`.
    3. Save both the validated and unvalidated dataset entries to
       "data/gpcrmd/{timestamp}/gpcrmd_datasets.parquet" and
       "data/gpcrmd/{timestamp}/not_validated_gpcrmd_datasets.parquet"
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
from bs4 import BeautifulSoup, Tag
from loguru import logger
from pydantic import ValidationError
from tqdm import tqdm

from models.dataset_model import DatasetModel, DatasetProject, DatasetRepository
from models.file_model import FileModel

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
        "Fetching entries from GPCRMD API... "
        "(usually takes less than 1 minute!)"
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


def fetch_entry_page(url: str) -> str | None:
    """Fetch an entry page and return its HTML content.

    Parameters
    ----------
    url : str
        The URL of the entry page to fetch.

    Returns
    -------
    str | None
        The HTML content of the page if the request is successful, otherwise None.
    """
    try:
        response = httpx.get(url, timeout=50)
        response.raise_for_status()
        # Sleep briefly to avoid overwhelming the remote server
        time.sleep(0.1)
    except httpx.HTTPStatusError as exc:
        logger.warning(f"HTTP error {exc.response.status_code} for {url}")
        return None
    except httpx.RequestError as exc:
        logger.warning(f"Request error for {url}: {exc}")
        return None

    return response.text


def retrieve_metadata(html: str, field_name: str, timeout: int = 50) -> str | None:
    """
    Retrieve a specific metadata field from a webpage.

    Parameters
    ----------
    html : str
        The HTML content of the page.
    field_name : str
        The name of the metadata field to extract (case-sensitive).
    timeout : int, optional
        Timeout in seconds for the HTTP request (default is 10).

    Returns
    -------
    str | None
        The value of the metadata field if found, otherwise None.

    """
    # Parse the HTML content of the page using BeautifulSoup
    if html:
        soup = BeautifulSoup(html, "html.parser")
        bold_tag = soup.find("b", string=lambda t: t and t.strip() == field_name)
        if not bold_tag:
            return None
        # Get all the text from the parent element of the <b> tag
        parent = bold_tag.parent
        if not isinstance(parent, Tag):
            return None
        parent_text = parent.get_text(strip=True)
        if ":" not in parent_text:
            return None
        # Get only what is after the "field_name:"
        metadata = parent_text.split(":", 1)[1].strip()
        return metadata
    return None


def retrieve_reference_links(html: str, timeout: int = 50) -> list[str] | None:
    """
    Retrieve reference URLs from the References section of a GPCRMD entry page.

    Parameters
    ----------
    html : str
        The HTML content of the page.
    timeout : int, optional
        Timeout in seconds for the HTTP request (default is 10).

    Returns
    -------
    list[str] | None
        List of reference URLs (starting with http:// or https://) if found,
        otherwise None.
    """
    if html:
        # Parse the HTML content
        soup = BeautifulSoup(html, "html.parser")
        # Find the <h3> header with text "References"
        header = soup.find("h3", string=lambda t: t and t.strip() == "References")
        if not header:
            return None
        # Get the corresponding content div containing the links
        content_div = header.find_next_sibling("div", class_="techinfo_content")
        if not content_div:
            return None

        # Iterate over all <a> elements with an href attribute inside the content div
        # Only keep elements that are of type Tag to satisfy type checkers
        content_div = header.find_next_sibling("div", class_="techinfo_content")
        if not isinstance(content_div, Tag):
            return None
        links: list[str] = []
        for a in filter(lambda x: isinstance(x, Tag), content_div.find_all("a", href=True)):
            href = a["href"].strip()
            # Only include links that start with "http://" or "https://"
            if href.startswith(("http://", "https://")):
                links.append(href)
        return links

    return None


def count_simulation_files(html: str, timeout: int = 50) -> int | None:
    """
    Count files in the dataset webpage.

    Especially in 'Simulation output files' and 'Simulation protocol \
    & starting files' sections.

    Parameters
    ----------
    html : str
        The HTML content of the page.

    Returns
    -------
    int | None
        The number of files related to this dataset.
    """
    if html:
        # Parse the HTML content
        soup = BeautifulSoup(html, "html.parser")

        # Helper function to count unique links in a container div
        def count_links(container_id: str) -> int:
            # Find the container <div> by ID
            container = soup.find("div", id=container_id)
            # Ensure the container is actually a Tag
            if not isinstance(container, Tag):
                return 0

            # Collect all hrefs in <a> tags, stripping whitespace
            links = [
                str(a.get("href", "")).strip()
                for a in container.find_all("a", href=True)
                if isinstance(a, Tag) and str(a.get("href", "")).strip()
            ]

            # Remove duplicates while preserving order
            return len(dict.fromkeys(links))

        output_files_count = count_links("allfiles")
        protocol_files_count = count_links("paramfiles")
        return output_files_count + protocol_files_count
    return None


def validate_parsed_entry(
    parsed_entry: dict[str, Any],
    out_model: type[FileModel | DatasetModel]
) -> tuple[FileModel | DatasetModel | None, dict[str, Any] | None]:
    """Validate a parsed entry using the pydantic model.

    Parameters
    ----------
    parsed_entry : dict[str, Any]
        The parsed entry to validate.
    out_model: FileModel | DatasetModel
        The Pydantic model used for the validation.

    Returns
    -------
    tuple[FileModel | DatasetModel | None,  dict[str, Any] | None]
        A tuple containing the validated model instance if validation succeeds,
        otherwise None, and the enriched parsed entry containing validation
        failure reasons if validation fails.
    """
    try:
        return out_model(**parsed_entry), None
    except ValidationError as exc:
        reasons: list[str] = []

        for err in exc.errors():
            field = ".".join(str(x) for x in err["loc"])
            reason = err["msg"]
            value = err.get("input")

            logger.error(
                "Validation error on '{}': value={!r} (type={}) -> {}",
                field,
                value,
                type(value).__name__,
                reason,
            )

            reasons.append(f"{field}: {reason}")

        parsed_entry["non_validation_reason"] = "; ".join(reasons)
        return None, parsed_entry


def parse_and_validate_entry_metadatas(
    entries: list[dict[str, Any]],
    fetch_time: str
) -> tuple[list[DatasetModel], list[dict[str, Any]]]:
    """
    Parse and validate metadata fields for a list of GPCRMD entries.

    Parameters
    ----------
    entries : list of dict
        List of dictionaries, each representing the metadata of a GPCRMD entry.
    fetch_time : str
        Timestamp (as a string) indicating when the data was fetched.

    Returns
    -------
    tuple[list[DatasetModel], list[dict[str, Any]]]
        - List of successfully validated `DatasetModel` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validating GPCRMD entries...")
    validated_entries: list[DatasetModel] = []
    non_validated_entries: list[dict[str, Any]] = []
    total_entries: int = len(entries)

    for entry in tqdm(entries,
            desc="Validating GPCRmd entries",
            colour="blue",
            unit="entry"
        ):
        entry_id = str(entry.get("dyn_id"))

        # Extract molecules and number total of atoms if available
        total_atoms: int | None = entry.get("atom_num")
        dyncomp: list[dict[str, Any]] = entry.get("dyncomp", [])
        molecules: list[str] = (
            [comp.get("resname") for comp in dyncomp if comp.get("resname")]
        )
        url: str = entry.get("url")
        # Fetch entry page with url
        html = fetch_entry_page(url)
        if html:
            author_names: str | None = retrieve_metadata(html, "Submitted by")
            description: str | None = retrieve_metadata(html, "Description")
            stime: str | None = retrieve_metadata(html, "Accumulated simulation time")
            refs: list[str] | None = retrieve_reference_links(html)
            nb_files: int | None = count_simulation_files(html)
        else:
            author_names = None
            description = None
            stime = None
            refs = None
            nb_files = None

        parsed_entry = {
            "dataset_repository": DatasetRepository.GPCRMD,
            "dataset_project": DatasetProject.GPCRMD,
            "dataset_id_in_repository": entry_id,
            "dataset_id_in_project": entry_id,
            "dataset_url_in_repository": url,
            "dataset_url_in_project": url,
            "links": refs,
            "title": entry.get("modelname"),
            "date_created": entry.get("creation_timestamp"),
            "date_last_fetched": fetch_time,
            "nb_files": nb_files,
            "author_names": author_names if author_names is None else [author_names],
            "description": description,
            "simulation_program_name": entry.get("mysoftware"),
            "simulation_program_version": entry.get("software_version"),
            "nb_atoms": total_atoms,
            "molecule_names": molecules,
            "forcefield_model_name": entry.get("forcefield"),
            "forcefield_model_version": entry.get("forcefield_version"),
            "timestep": entry.get("timestep"),
            "delta": entry.get("delta"),
            "simulation_time": stime
            }

        # Validate and normalize data collected with pydantic model
        (dataset_model_entry,
            non_validated_parsed_entry,
        ) = validate_parsed_entry(parsed_entry, DatasetModel)
        if isinstance(dataset_model_entry, DatasetModel):
            validated_entries.append(dataset_model_entry)
        if non_validated_parsed_entry:
            non_validated_entries.append(non_validated_parsed_entry)

    percentage = (
        (len(validated_entries) / total_entries) * 100
        if total_entries > 0
        else 0.0
    )
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_entries)}/{total_entries}) datasets successfully! \n"
    )
    return validated_entries, non_validated_entries


def make_base_parsed_entry(
    entry_id: str,
    url: str,
    fetch_time: str,
) -> dict[str, Any]:
    """Create a base parsed entry with empty file metadata.

    Parameters
    ----------
    entry_id : str
        The unique identifier of the GPCRMD entry.
    url : str
        The URL of the GPCRMD entry.
    fetch_time : str
        The timestamp indicating when the data was fetched.

    Returns
    -------
    dict[str, Any]
        A dictionary representing the base parsed entry with empty file metadata.
    """
    return {
        "dataset_repository": DatasetRepository.GPCRMD,
        "dataset_id_in_repository": entry_id,
        "file_name": None,
        "file_type": None,
        "file_size_in_bytes": None,
        "file_url_in_repository": url,
        "date_last_fetched": fetch_time,
    }


def fetch_file_size(file_path: str) -> int | None:
    """Fetch file size using a HEAD request.

    Parameters
    ----------
    file_path : str
        The URL of the file to fetch the size for.

    Returns
    -------
        int | None
        The size of the file in bytes if available, otherwise None.
    """
    try:
        response = httpx.head(file_path, timeout=50, follow_redirects=True)
        # Sleep briefly to avoid overwhelming the remote server
        time.sleep(0.1)
        return int(response.headers.get("Content-Length", 0))
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "HTTP error %s for %s",
            exc.response.status_code,
            file_path,
        )
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch file size for %s: %s", file_path, exc)

    return None


def fetch_and_validate_file_metadatas(
    entries: list[dict],
    fetch_time: str,
) -> tuple[list[FileModel], list[dict[str, Any]]]:
    """Fetch and validate metadata for GPCRMD files.

    Parameters
    ----------
    entries : list[dict]
        List of file entries, each containing metadata such as 'dyn_id' and 'url'.
    fetch_time : str
        Timestamp indicating when the data was fetched.

    Returns
    -------
    tuple[list[DatasetModel], list[dict[str, Any]]]
        - List of successfully validated `FileModel` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting fetching and validating GPCRMD files...")

    validated_files: list[FileModel] = []
    non_validated_files: list[dict] = []
    total_files = 0

    for entry in tqdm(
        entries,
        desc="Validating GPCRmd files",
        colour="blue",
        unit="file",
    ):
        entry_id = str(entry.get("dyn_id"))
        url = entry.get("url")

        base_entry = make_base_parsed_entry(entry_id, url, fetch_time)

        html = fetch_entry_page(url)
        if html is None:
            base_entry["non_validation_reason"] = "entry_page_fetch_failed"
            non_validated_files.append(base_entry)
            continue

        soup = BeautifulSoup(html, "html.parser")

        for sec_id in ("allfiles", "paramfiles"):
            container = soup.find("div", id=sec_id)
            # Ensure container is a Tag
            if not isinstance(container, Tag):
                continue

            links = container.find_all("a", href=True)
            total_files += len(links)

            for link in links:
                # Ensure link is a Tag to safely access ['href']
                if not isinstance(link, Tag):
                    continue

                # Use .get() to safely retrieve the href, then convert to str
                href_value = str(link.get("href", "")).strip()
                if not href_value:
                    continue
                file_path = f"https://www.gpcrmd.org/{href_value}"
                file_name = os.path.basename(file_path)
                file_type = os.path.splitext(file_name)[1].lstrip(".").lower()

                parsed_entry = {
                    **base_entry,
                    "file_name": file_name,
                    "file_type": file_type,
                    "file_size_in_bytes": fetch_file_size(file_path),
                    "file_url_in_repository": file_path,
                }

                # Validate and normalize data collected with pydantic model
                (file_model_entry,
                    non_validated_parsed_entry,
                ) = validate_parsed_entry(parsed_entry, FileModel)
                if isinstance(file_model_entry, FileModel):
                    validated_files.append(file_model_entry)
                if non_validated_parsed_entry:
                    non_validated_files.append(non_validated_parsed_entry)

    percentage = (
        (len(validated_files) / total_files) * 100
        if total_files > 0
        else 0.0
    )
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_files) - len(non_validated_files)}/"
        f"{total_files}) files successfully! \n"
    )
    return validated_files, non_validated_files


def save_metadatas_to_parquet(
    folder_out_path: Path,
    metadatas_validated: list[DatasetModel] | list[FileModel],
    metadatas_unvalidated: list[dict],
    tag: str,
) -> None:
    """
    Save GPCRMD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    folder_out_path : Path
        Folder path where Parquet files will be saved.
    metadatas_validated : List[DatasetModel]
        List of validated metadatas.
    metadatas_unvalidated : List[Dict]
        List of unvalidated metadatas as dictionaries.
    tag: str
        Tag to know if its entries or files metadata to save.
    """
    logger.info("Saving GPCRMD entries metadatas to a Parquet file...")
    # Ensure output folder exists
    Path(folder_out_path).mkdir(parents=True, exist_ok=True)

    # Save validated entries
    if tag == "entries":
        validated_path = os.path.join(folder_out_path, "gpcrmd_datasets.parquet")
    elif tag == "files":
        validated_path = os.path.join(folder_out_path, "gpcrmd_files.parquet")
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
            folder_out_path, "not_validated_gpcrmd_datasets.parquet"
        )
    elif tag == "files":
        unvalidated_path = os.path.join(
            folder_out_path, "not_validated_gpcrmd_files.parquet"
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
    # Parse and validate  entry metadatas with a pydantic model (DatasetModel)
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

    # Fetch, parse and validate the file metadatas with a pydantic model (File Model)
    files_metadata_validated, files_metadata_unvalidated = (
        fetch_and_validate_file_metadatas(entries, fetch_time)
    )
    save_metadatas_to_parquet(
        out_path,
        files_metadata_validated,
        files_metadata_unvalidated,
        tag="files"
    )

    # Compute the elapsed time for scrapping
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

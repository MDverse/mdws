"""
Scrape datasets and files from GPCRMD.

This script fetches datasets from the GPCRMD repository (https://www.gpcrmd.org/).
It collects metadata such as dataset names, descriptions, authors, download links,
and other relevant information for all available datasets.
Additionally, it retrieves file metadata for each dataset, including file paths
in GPCRMD, file size, type/extension, etc.

The scraped data is validated against Pydantic models (`DatasetMetadata`
and `File Model`) and saved locally in Parquet format:
- "data/gpcrmd/gpcrmd_datasets.parquet"
- "data/gpcrmd/gpcrmd_files.parquet"

Usage:
======
    uv run -m scripts.scrap_gpcrmd [--out-path]

Arguments:
==========
    --out-path : (optional)
        Folder path to save the scraped GPCRMD data (dataset and file metadata).
        Default is "data/gpcrmd".

Example:
========
   uv run -m scripts.scrap_gpcrmd

This command will:
    1. Fetch all available datasets from GPCRMD.
    2. Parse their metadata and validate them using the Pydantic models
    `DatasetMetadata` and `FileMetadata`.
    3. Save both the validated dataset datasets to "data/gpcrmd/gpcrmd_datasets.parquet"
    4. Save file metadata similarly for validated files.
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import httpx
import pandas as pd
from bs4 import BeautifulSoup, Tag
from loguru import logger
from pydantic import ValidationError

from models.dataset import DatasetMetadata, DatasetProject, DatasetRepository
from models.file import FileMetadata

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
        mode="w",
    )
    loguru_logger.add(
        sys.stdout,
        format=fmt,
        level="DEBUG",
    )


def fetch_datasets_once() -> tuple[list[dict[str, Any]], str]:
    """
    Fetch all datasets from the GPCRMD API.

    Returns
    -------
    Tuple[List[Dict[str, Any]], str]:
        - A list of datasets (JSON objects).
        Returns an empty list if the request fails.
        - The current timestamp in ISO 8601 format (e.g., '2023-03-05T22:01:12').
    """
    logger.debug(
        "Fetching datasets from GPCRMD API... "
        "(usually takes less than 1 minute!)"
    )
    # Current timestamp in ISO format
    fetch_time: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        # Perform the HTTP GET request with a long timeout to accommodate large data
        response = httpx.get(BASE_GPCRMD_URL, timeout=1000)
        response.raise_for_status()

        # Parse JSON data
        datasets = response.json()
        logger.success(
            f"Fetched {len(datasets)} MD-related datasets from GPCRMD successfully! \n"
        )
        return datasets, fetch_time

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], fetch_time


def fetch_dataset_page(url: str) -> str | None:
    """Fetch an dataset page and return its HTML content.

    Parameters
    ----------
    url : str
        The URL of the dataset page to fetch.

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


def retrieve_metadata(html: str, field_name: str) -> str | None:
    """
    Retrieve a specific metadata field from a webpage.

    Parameters
    ----------
    html : str
        The HTML content of the page.
    field_name : str
        The name of the metadata field to extract (case-sensitive).

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


def retrieve_reference_links(html: str) -> list[str] | None:
    """
    Retrieve reference URLs from the References section of a GPCRMD dataset page.

    Parameters
    ----------
    html : str
        The HTML content of the page.

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
        for a in filter(lambda x: isinstance(x, Tag),
            content_div.find_all("a", href=True)):
            href = a["href"].strip()
            # Only include links that start with "http://" or "https://"
            if href.startswith(("http://", "https://")):
                links.append(href)
        return links

    return None


def count_simulation_files(html: str) -> int | None:
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


def validate_parsed_metadatas(
    parsed: dict[str, Any],
    out_model: type[FileMetadata | DatasetMetadata]
) -> tuple[FileMetadata | DatasetMetadata | None, str | None]:
    """Validate a parsed dataset using the pydantic model.

    Parameters
    ----------
    parsed : dict[str, Any]
        The parsed dataset or file to validate.
    out_model: FileMetadata | DatasetMetadata
        The Pydantic model used for the validation.

    Returns
    -------
    tuple[FileMetadata | DatasetMetadata | None,  str | None]
        A tuple containing the validated model instance if validation succeeds,
        otherwise None, and the validation failure reasons if validation fails.
    """
    try:
        return out_model(**parsed), None
    except ValidationError as exc:
        reasons: list[str] = []

        for err in exc.errors():
            field = ".".join(str(x) for x in err["loc"])
            reason = err["msg"]
            value = err.get("input")

            reasons.append(f"{field}: {reason} (input={value!r})")

        non_validation_reason = "; ".join(reasons)
        return None, non_validation_reason


def parse_and_validate_dataset_metadatas(
    datasets: list[dict[str, Any]],
    fetch_time: str
) -> list[DatasetMetadata]:
    """
    Parse and validate metadata fields for a list of GPCRMD datasets.

    Parameters
    ----------
    datasets : list of dict
        List of dictionaries, each representing the metadata of a GPCRMD dataset.
    fetch_time : str
        Timestamp (as a string) indicating when the data was fetched.

    Returns
    -------
    list[DatasetMetadata]
        List of successfully validated `DatasetMetadata` objects.
    """
    logger.info("Starting parsing and validating GPCRMD datasets...")
    validated_datasets: list[DatasetMetadata] = []
    total_datasets: int = len(datasets)

    for i, dataset in enumerate(datasets, start=1):
        dataset_id = str(dataset.get("dyn_id"))

        # Extract molecules and number total of atoms if available
        total_atoms: int | None = dataset.get("atom_num")
        dyncomp: list[dict[str, Any]] = dataset.get("dyncomp", [])
        molecules: list[str] = (
            [comp.get("resname") for comp in dyncomp if comp.get("resname")]
        )
        url = dataset.get("url")
        # Fetch dataset page with url
        html = fetch_dataset_page(url) if url else None
        if html:
            author_names: str | None = retrieve_metadata(html, "Submitted by")
            description: str | None = retrieve_metadata(html, "Description")
            stime: str | None = retrieve_metadata(html, "Accumulated simulation time")
            stime_list: list[str] | None = [stime] if stime is not None else None
            refs: list[str] | None = retrieve_reference_links(html)
            nb_files: int | None = count_simulation_files(html)
        else:
            logger.warning(f"Dataset `{dataset_id}` ({url}): "
                           "page HTML missing; web metadata extraction skipped.")
            author_names = None
            description = None
            stime = None
            refs = None
            nb_files = None

        parsed_dataset = {
            "dataset_repository_name": DatasetRepository.GPCRMD,
            "dataset_project": DatasetProject.GPCRMD,
            "dataset_id_in_repository": dataset_id,
            "dataset_id_in_project": dataset_id,
            "dataset_url_in_repository": url,
            "dataset_url_in_project": url,
            "links": refs,
            "title": dataset.get("modelname"),
            "date_created": dataset.get("creation_timestamp"),
            "date_last_fetched": fetch_time,
            "nb_files": nb_files,
            "author_names": author_names if author_names is None else [author_names],
            "description": description,
            "simulation_program_name": dataset.get("mysoftware"),
            "simulation_program_version": dataset.get("software_version"),
            "nb_atoms": total_atoms,
            "molecule_names": molecules,
            "forcefield_model_name": dataset.get("forcefield"),
            "forcefield_model_version": dataset.get("forcefield_version"),
            "timestep": dataset.get("timestep"),
            "delta": dataset.get("delta"),
            "simulation_time": stime_list
            }

        # Validate and normalize data collected with pydantic model
        (parsed_dataset_model,
            non_validation_reason,
        ) = validate_parsed_metadatas(parsed_dataset, DatasetMetadata)
        # If it return a DatasetMetadata object
        if isinstance(parsed_dataset_model, DatasetMetadata):
            # Validation succeed
            logger.debug(f"Parsed dataset id `{dataset_id}` ({i}/{len(datasets)})")
            validated_datasets.append(parsed_dataset_model)
        else:
            # Validation failed
            logger.error(f"Validation failed for dataset `{dataset_id}` ({url})"
                                ". Invalid field(s) detected : "
                                f"{non_validation_reason}"
                    )

    percentage = (
        (len(validated_datasets) / total_datasets) * 100
        if total_datasets > 0
        else 0.0
    )
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_datasets)}/{total_datasets}) datasets successfully! \n"
    )
    return validated_datasets


def make_base_parsed_file(
    dataset_id: str,
    url: str,
    fetch_time: str,
) -> dict[str, Any]:
    """Create a base parsed dataset with empty file metadata.

    Parameters
    ----------
    dataset_id : str
        The unique identifier of the GPCRMD dataset.
    url : str
        The URL of the GPCRMD dataset.
    fetch_time : str
        The timestamp indicating when the data was fetched.

    Returns
    -------
    dict[str, Any]
        A dictionary representing the base parsed dataset with empty file metadata.
    """
    return {
        "dataset_repository_name": DatasetRepository.GPCRMD,
        "dataset_id_in_repository": dataset_id,
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
    datasets: list[dict],
    fetch_time: str,
) -> list[FileMetadata]:
    """Fetch and validate metadata for GPCRMD files.

    Parameters
    ----------
    datasets : list[dict]
        List of file datasets, each containing metadata such as 'dyn_id' and 'url'.
    fetch_time : str
        Timestamp indicating when the data was fetched.

    Returns
    -------
    list[FileMetadata]
        List of successfully validated `FileMetadata` objects.
    """
    logger.info("Starting fetching and validating GPCRMD files...")

    validated_files: list[FileMetadata] = []
    total_files = 0
    non_validated_files_count = 0

    for i, dataset in enumerate(datasets, start=1):
        dataset_id = str(dataset.get("dyn_id"))
        url = dataset.get("url")
        count_files_parsed_for_dataset = 0

        if not url:
            logger.error(
                    f"Dataset `{dataset_id}` skipped: missing dataset URL."
                )
            continue

        base_file = make_base_parsed_file(dataset_id, url, fetch_time)

        html = fetch_dataset_page(url) if url else None
        if html is None:
            logger.error(
                f"Dataset `{dataset_id}` ({url}) skipped: page retrieval failed."
            )
            continue

        soup = BeautifulSoup(html, "html.parser")

        for sec_id in ("allfiles", "paramfiles"):
            container = soup.find("div", id=sec_id)
            # Ensure container is a Tag
            if not isinstance(container, Tag):
                if sec_id == "allfiles":
                    # allfiles mandatory
                    logger.warning(
                        f"Dataset id `{dataset_id}` ({url}):"
                        f"mandatory section `{sec_id}` is missing or invalid. "
                        "Files required for simulation parsing cannot be retrieved."
                    )
                else:
                    # paramfiles optional
                    # logger.warning(
                    #     f"Dataset id `{dataset_id}` ({url}): "
                    #     f"optional section `{sec_id}` not found. "
                    #     "Parameter files for simulations will be skipped."
                    # )
                    pass
                continue

            links = container.find_all("a", href=True)
            total_files += len(links)

            for link in links:
                # Ensure link is a Tag to safely access ['href']
                if not isinstance(link, Tag):
                    logger.warning(
                        f"Dataset `{dataset_id}` ({url}): "
                        "encountered non-HTML link element."
                    )
                    continue

                # Use .get() to safely retrieve the href, then convert to str
                href_value = str(link.get("href", "")).strip()
                if not href_value:
                    logger.warning(
                        f"Dataset `{dataset_id}` ({url}): "
                        "file link without href attribute."
                    )
                    continue
                file_path = f"https://www.gpcrmd.org/{href_value}"
                file_name = os.path.basename(file_path)
                file_type = os.path.splitext(file_name)[1].lstrip(".").lower()

                parsed_file = {
                    **base_file,
                    "file_name": file_name,
                    "file_type": file_type,
                    "file_size_in_bytes": fetch_file_size(file_path),
                    "file_url_in_repository": file_path,
                }

                # Validate and normalize data collected with pydantic model
                (parsed_file_model,
                    non_validation_reason,
                ) = validate_parsed_metadatas(parsed_file, FileMetadata)
                count_files_parsed_for_dataset += 1
                if isinstance(parsed_file_model, FileMetadata):
                    logger.debug(
                        f"Parsed file `{file_name}` from dataset "
                        f"`{dataset_id}` ({i}/{len(datasets)})"
                    )
                    validated_files.append(parsed_file_model)
                else:
                    logger.error(f"Validation failed for file `{file_name}` "
                                 f"({file_path}). Invalid field(s) detected : "
                                f"{non_validation_reason}"
                    )
                    non_validated_files_count += 1

    percentage = (
        (len(validated_files) / total_files) * 100
        if total_files > 0
        else 0.0
    )
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_files) - non_validated_files_count}/"
        f"{total_files}) files successfully! \n"
    )
    return validated_files


def save_metadatas_to_parquet(
    folder_out_path: Path,
    metadatas_validated: list[DatasetMetadata] | list[FileMetadata],
    tag: str,
) -> None:
    """
    Save GPCRMD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    folder_out_path : Path
        Folder path where Parquet files will be saved.
    metadatas_validated : List[DatasetMetadata]
        List of validated metadatas.
    tag: str
        Tag to know if its datasets or files metadata to save.
    """
    logger.info(f"Saving GPCRMD {tag} metadatas to a Parquet file...")
    # Ensure output folder exists
    Path(folder_out_path).mkdir(parents=True, exist_ok=True)

    # Save validated datasets and files
    if tag == "datasets":
        validated_path = os.path.join(folder_out_path, "gpcrmd_datasets.parquet")
    elif tag == "files":
        validated_path = os.path.join(folder_out_path, "gpcrmd_files.parquet")
    try:
        # Convert list of Pydantic models to list of dicts
        validated_dicts = [dataset.model_dump() for dataset in metadatas_validated]
        df_validated = pd.DataFrame(validated_dicts)
        df_validated.to_parquet(validated_path, index=False)
        logger.success(
            f"GPCRMD validated metadatas saved to: {validated_path} successfully!"
        )
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save validated {tag} to {validated_path}: {e}")


@click.command()
@click.option(
    "--out-path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    default=Path("data/gpcrmd"),
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
    start_time = time.perf_counter()

    # Fetch datasets metadata
    datasets, fetch_time = fetch_datasets_once()
    if datasets == []:
        logger.warning("No data fetched from GPCRMD.")
        return
    # Parse and validate dataset metadatas with a pydantic model (DatasetMetadata)
    datasets_validated = (
        parse_and_validate_dataset_metadatas(datasets, fetch_time)
    )
    # Save parsed metadata to local file
    save_metadatas_to_parquet(
        out_path,
        datasets_validated,
        tag="datasets"
    )

    # Fetch, parse and validate the file metadatas with a pydantic model (File Model)
    files_metadata_validated = (
        fetch_and_validate_file_metadatas(datasets, fetch_time)
    )
    save_metadatas_to_parquet(
        out_path,
        files_metadata_validated,
        tag="files"
    )

    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraping duration: {timedelta(seconds=elapsed_time)} 🎉")


if __name__ == "__main__":
    # Scrap GPCRMD data
    scrap_gpcrmd_data()

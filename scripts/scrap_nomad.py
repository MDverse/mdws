"""Scrap molecular dynamics simulation datasets and files from NOMAD.

This script fetches molecular dynamics (MD) datasets from the NOMAD repository (https://nomad-lab.eu/prod/v1/gui/search/entries).
It collects metadata such as dataset names, descriptions, authors, download links...
for datasets related to molecular dynamics simulations.
Additionally, it retrieves file metadata for each dataset, including file paths
in NOMAD,size, file type/extension... of molecular dynamics simulations.

The scraped data is validated against Pydantic models (`DatasetModel` and `FileModel`)
and saved locally in Parquet format:
- "data/nomad/{timestamp}/nomad_datasets.parquet"
- "data/nomad/{timestamp}/nomad_files.parquet"

Entries that fail validation are saved as:
- "data/nomad/{timestamp}/not_validated_nomad_datasets.parquet"
- "data/nomad/{timestamp}/not_validated_nomad_files.parquet"


Usage :
=======
    uv run -m scripts.scrap_nomad [--out-path]

Arguments:
==========
    --out-path : (optional)
        Folder path to save the scraped NOMAD data (Dataset and File metadatas).
        Default is "data/nomad/{timestamp}".

Example:
========
    uv run -m scripts.scrap_nomad

This command will:
    1. Fetch molecular dynamics entries from the NOMAD API in batches of 50.
    2. Parse their metadata and validate them using the Pydantic models `DatasetModel`
       and `FileModel`.
    3. Save both the validated and unvalidated entries to "data/nomad/{timestamp}/
       {validated or unvalidated}_entries.parquet".
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
from loguru import logger
from pydantic import ValidationError
from tqdm import tqdm

from models.dataset_model import DatasetModel, DatasetProject, DatasetRepository
from models.file_model import FileModel

# CONSTANTS
BASE_NOMAD_URL = "http://nomad-lab.eu/prod/v1/api/v1"
JSON_PAYLOAD_NOMAD_REQUEST = {
    "owner": "visible",
    "query": {"results.method.workflow_name:any": ["MolecularDynamics"]},
    "aggregations": {},
    "pagination": {
        "order_by": "upload_create_time",
        "order": "desc",
        "page_size": None,
    },
    "required": {
        "exclude": [
            "quantities",
            "sections",
        ]
    },
}


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
        log_folder / "scrap_nomad_data_{time:YYYY-MM-DD}.log",
        format=fmt,
        level="DEBUG",
    )
    loguru_logger.add(
        sys.stdout,
        format=fmt,
        level="DEBUG",
    )


def test_nomad_connection() -> bool | None:
    """Test connection to the NOMAD API.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to NOMAD API...")
    try:
        r = httpx.get(f"{BASE_NOMAD_URL}/entries", timeout=5)
        if r.status_code == 200:
            logger.success("Connected to NOMAD API successfully!")
            return True
    except httpx.HTTPStatusError as exc:
        logger.warning(f"HTTP error {exc.response.status_code}")
        return False
    except httpx.RequestError as exc:
        logger.warning(f"Request error {exc}")
        return False


def fetch_nomad_md_related_by_batch(
    query_entry_point: str, tag: str, page_size: int = 50
) -> list[tuple[list[dict[str, Any]], str]]:
    """
    Fetch all Molecular Dynamics (MD)-related entries/files from the NOMAD API by batch.

    Parameters
    ----------
    query_entry_point : str
        The entry point of the .post API request.
        Doc: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata
    tag: str
        Tag to know if its entries or files metadata to fetch.
    page_size : int
        Number of entries to fetch per page. (Default : 50)

    Returns
    -------
    List[Tuple[List[Dict[str, Any]], str]]:
        - A list of tuples (entries_list, fetch_time) for each batch.
    """
    logger.info(
        f"Fetching Molecular Dynamics related {tag} from NOMAD API \
        by batch of {page_size} {tag}..."
    )
    all_entries_with_time = []
    next_page_value = None
    total_entries = None

    # Fetch the first page
    try:
        logger.debug("Requesting first page...")
        fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # HTTP request
        response = httpx.post(
            f"{BASE_NOMAD_URL}/{query_entry_point}",
            json={
                **JSON_PAYLOAD_NOMAD_REQUEST,
                "pagination": {
                    **JSON_PAYLOAD_NOMAD_REQUEST["pagination"],
                    "page_size": page_size,
                },
            },
            timeout=100,
        )
        response.raise_for_status()
        # Get the formated response with request metadatas in JSON format
        first_entries_with_request_md = response.json()
        # Get the total entries from the request md
        total_entries = first_entries_with_request_md["pagination"]["total"]
        # Get the ID to start the next batch of entries
        next_page_value = first_entries_with_request_md["pagination"][
            "next_page_after_value"
        ]
        # Get only the entries metadatas
        first_entries = first_entries_with_request_md["data"]
        # Add it with the crawled time
        all_entries_with_time.append((first_entries, fetch_time))
        logger.debug(
            f"Fetched first {len(first_entries_with_request_md['data'])}\
            /{total_entries} {tag}"
        )

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [([], "")]

    # Paginate through remaining entries
    logger.debug(
        f"Paginate through remaining {tag}... (usually takes around 3 minutes)"
    )
    with tqdm(
        total=total_entries if tag == "entries" else None,
        desc=f"Fetching MD {tag} from NOMAD",
        colour="blue",
        ncols=100,
        ascii="░▒█",
        unit="entry" if tag == "entries" else "file",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, \
                    {rate_fmt}]",
    ) as pbar:
        # Initial update for the first batch already fetched
        pbar.update(len(first_entries))
        while next_page_value:
            try:
                fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                # HTTP request
                response = httpx.post(
                    f"{BASE_NOMAD_URL}/{query_entry_point}",
                    json={
                        **JSON_PAYLOAD_NOMAD_REQUEST,
                        "pagination": {
                            **JSON_PAYLOAD_NOMAD_REQUEST["pagination"],
                            "page_size": page_size,
                            "page_after_value": next_page_value,
                        },
                    },
                    timeout=100,
                )
                response.raise_for_status()
                next_batch = response.json()
                all_entries_with_time.append((next_batch["data"], fetch_time))

                # Update the bar progression
                pbar.update(len(next_batch["data"]))
                # Update the next entry to begin with
                next_page_value = next_batch.get("pagination", {}).get(
                    "next_page_after_value", None
                )
            except httpx.HTTPError as e:
                logger.error(f"HTTP error occurred while fetching next page: {e}")
                break

    total_datasets = sum(len(batch[0]) for batch in all_entries_with_time)
    total_files = sum(
        len(entry["files"]) for batch, _ in all_entries_with_time for entry in batch
    )
    logger.success(
        f"Fetched {total_datasets if tag == 'entries' else total_files} Molecular \
        Dynamics {tag} from NOMAD successfully! \n"
    )
    return all_entries_with_time


def fetch_entries_md_related_once() -> tuple[list[dict[str, Any]], str]:
    # NOTE: Deprecated -> single-request fetch can be too heavy; prefer batch fetching
    """
    Fetch all Molecular Dynamics-related entries from the NOMAD API in a single request.

    Note:
        This function is not recommended for regular use, as the server must prepare
        approximately 1.5 GB of data in one batch, which can be slow/memory-intensive.

    Returns
    -------
    Tuple[List[Dict[str, Any]], str]:
        - A list of entries related to Molecular Dynamics workflows (JSON objects).
        Returns an empty list if the request fails.
        - The current timestamp in ISO 8601 format (e.g., '2023-03-05T22:01:12').

    """
    logger.debug(
        "Fetching Molecular Dynamics related entries from NOMAD API... (usually take \
        less than 3 minutes!)"
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

        # Perform the HTTP GET request with a long timeout to accommodate large data
        response = httpx.get(url, timeout=1000)
        response.raise_for_status()

        # Parse JSON data
        entries_md = response.json()
        logger.success(
            f"Fetched {len(entries_md)} MD-related entries from NOMAD successfully! \n"
        )
        return entries_md, fetch_time

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [], fetch_time


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
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> tuple[list[DatasetModel], list[dict]]:
    """
    Parse and validate metadata fields for all NOMAD entries in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of entries and the fetch_time.

    Returns
    -------
    Tuple[List[DatasetModel], List[Dict]]
        - List of successfully validated `DatasetModel` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validation of NOMAD entries...")
    validated_entries = []
    non_validated_entries = []
    total_entries = sum(len(batch) for batch, _ in nomad_data)

    for entries_list, fetch_time in nomad_data:
        for data in entries_list:
            entry_id = data.get("entry_id")

            # Extract molecules and number total of atoms if available
            total_atoms = None
            molecules = None
            try:
                topology = (
                    data.get("results", {}).get("material", {}).get("topology", [])
                )
                if isinstance(topology, list):
                    total_atoms = next(
                        (
                            t.get("n_atoms")
                            for t in topology
                            if t.get("label") == "original"
                        ),
                        None,
                    )
                    molecules = [
                        f"{t.get('label')} ({t.get('n_atoms')} atoms)"
                        for t in topology
                        if t.get("structural_type") == "molecule"
                    ]
            except (ValueError, KeyError) as e:
                logger.warning(f"Error parsing molecules for entry {entry_id}: {e}")

            parsed_entry = {
                "dataset_repository": DatasetRepository.NOMAD,
                "dataset_project": DatasetProject.NOMAD,
                "dataset_id_in_repository": entry_id,
                "dataset_id_in_project": entry_id,
                "dataset_url_in_repository": f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}",
                "dataset_url_in_project": f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}",
                "links": data.get("references"),
                "title": data.get("entry_name"),
                "date_created": data.get("entry_create_time"),
                "date_last_updated": data.get("last_processing_time"),
                "date_last_fetched": fetch_time,
                "nb_files": len(data.get("files", [])),
                "author_names": [a.get("name") for a in data.get("authors", [])],
                "license": data.get("license"),
                "description": data.get("comment"),
                "simulation_program_name": (
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
                ),
                "nb_atoms": total_atoms,
                "molecule_names": molecules,
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


def parse_and_validate_files_metadatas(
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> tuple[list[FileModel], list[dict]]:
    """
    Parse and validate metadata fields for all NOMAD files in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of files and the fetch_time.

    Returns
    -------
    Tuple[List[DatasetModel], List[Dict]]
        - List of successfully validated `FileModel` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validation of NOMAD files...")
    validated_files = []
    non_validated_files = []
    total_files = sum(len(entry["files"]) for batch, _ in nomad_data for entry in batch)

    for entries_list, fetch_time in nomad_data:
        for data in entries_list:
            entry_id = data.get("entry_id")
            for file in data.get("files", []):
                name_file = file["path"].split("/")[-1]
                file_extension = name_file.split(".")[-1]
                file_path = (
                    f"https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/"
                    f"{entry_id}/files/{name_file}"
                )
                size = file.get("size", None)

                parsed_entry = {
                    "dataset_repository": DatasetRepository.NOMAD,
                    "dataset_id_in_repository": entry_id,
                    "file_name": name_file,
                    "file_type": file_extension,
                    "file_size_in_bytes": size,
                    "file_url_in_repository": file_path,
                    "date_last_fetched": fetch_time,
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
        f"({len(validated_files)}/{total_files}) files successfully! \n"
    )
    return validated_files, non_validated_files


def save_nomad_entries_metadatas_to_parquet(
    folder_out_path: Path,
    nomad_metadatas_validated: list[DatasetModel] | list[FileModel],
    nomad_metadatas_unvalidated: list[dict],
    tag: str,
) -> None:
    """
    Save NOMAD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    folder_out_path : Path
        Folder path where Parquet files will be saved.
    nomad_metadatas_validated : List[DatasetModel]
        List of validated NOMAD entries.
    nomad_metadatas_unvalidated : List[Dict]
        List of unvalidated NOMAD entries as dictionaries.
    tag: str
        Tag to know if its entries or files metadata to save.
    """
    logger.info("Saving NOMAD entries metadatas to a Parquet file...")
    # Ensure output folder exists
    Path(folder_out_path).mkdir(parents=True, exist_ok=True)

    # Save validated entries
    if tag == "entries":
        validated_path = os.path.join(folder_out_path, "nomad_datasets.parquet")
    elif tag == "files":
        validated_path = os.path.join(folder_out_path, "nomad_files.parquet")
    try:
        # Convert list of Pydantic models to list of dicts
        validated_dicts = [entry.model_dump() for entry in nomad_metadatas_validated]
        df_validated = pd.DataFrame(validated_dicts)
        df_validated.to_parquet(validated_path, index=False)
        logger.success(
            f"NOMAD validated metadatas saved to: {validated_path} successfully!"
        )
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save validated metadata to {validated_path}: {e}")

    # Save unvalidated entries
    if tag == "entries":
        unvalidated_path = os.path.join(
            folder_out_path, "not_validated_nomad_datasets.parquet"
        )
    elif tag == "files":
        unvalidated_path = os.path.join(
            folder_out_path, "not_validated_nomad_files.parquet"
        )
    try:
        if len(nomad_metadatas_unvalidated) != 0:
            df_unvalidated = pd.DataFrame(nomad_metadatas_unvalidated)
            df_unvalidated.to_parquet(unvalidated_path, index=False)
            logger.success(
            f"NOMAD not validated metadatas saved to: {unvalidated_path} successfully!"
            )
        else:
            logger.warning("There is no not validated entries to save!")
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save not validated metadata to {unvalidated_path}: {e}")


@click.command()
@click.option(
    "--out-path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    default=Path(f"data/nomad/{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
    show_default=True,
    help="Folder path to save the scraped NOMAD data (Dataset and File metadatas)"
)
def scrap_nomad_data(out_path: Path) -> None:
    """Scrap molecular dynamics datasets and files from NOMAD.

    Parameters
    ----------
    out_path : Path
        The output folder path for the scraped data.
    """
    setup_logger(logger, out_path)
    logger.info("Starting Nomad data scraping...")
    start_time = time.time()

    if test_nomad_connection:
        # Fetch NOMAD entries metadata
        nomad_data = fetch_nomad_md_related_by_batch(
            query_entry_point="entries/query", tag="entries"
        )
        if nomad_data == []:
            logger.warning("No data fetched from NOMAD.")
            return
        # Parse and validate NOMAD entry metadatas with a pydantic model (DatasetModel)
        nomad_entries_validated, nomad_entries_unvalidated = (
            parse_and_validate_entry_metadatas(nomad_data)
        )
        # Save parsed metadata to local file
        save_nomad_entries_metadatas_to_parquet(
            out_path,
            nomad_entries_validated,
            nomad_entries_unvalidated,
            tag="entries",
        )

        # Fetch NOMAD files metadata
        nomad_files_metadata = fetch_nomad_md_related_by_batch(
            query_entry_point="entries/rawdir/query", tag="files"
        )
        # Parse and validate the file metadatas with a pydantic model (FileModel)
        nomad_files_metadata_validated, nomad_files_metadata_unvalidated = (
            parse_and_validate_files_metadatas(nomad_files_metadata)
        )
        save_nomad_entries_metadatas_to_parquet(
            out_path,
            nomad_files_metadata_validated,
            nomad_files_metadata_unvalidated,
            tag="files",
        )

        end_time = time.time()
        elapsed_time = end_time - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)

        logger.success(
            f"Completed Nomad data scraping in {hours} h {minutes} min {seconds} sec 🎉"
        )

    else:
        logger.error("Cannot scrap data, no connection to NOMAD API.")
        sys.exit()


if __name__ == "__main__":
    # Scrap NOMAD data
    scrap_nomad_data()

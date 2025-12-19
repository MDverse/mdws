"""Scrap molecular dynamics datasets and files from NOMAD.

This script fetches molecular dynamics (MD) datasets from the NOMAD repository (https://nomad-lab.eu/prod/v1/gui/search/entries).
It collects metadata such as dataset names, descriptions, authors, download links...
for datasets related to molecular dynamics simulations.
Additionally, it retrieves file metadata for each dataset, including file paths
in NOMAD,size, file type/extension... of molecular dynamics simulations.

The scraped data is validated against Pydantic models (`NomadDataset` and `NomadFile`)
and saved locally in Parquet format:
- "data/nomad/{timestamp}/validated_entries.parquet"
- "data/nomad/{timestamp}/validated_files.parquet"

Entries that fail validation are saved as:
- "data/nomad/{timestamp}/unvalidated_entries.parquet"
- "data/nomad/{timestamp}/unvalidated_files.parquet"


Usage :
=======
    uv run scripts/scrap_nomad.py [--out-path]

Arguments:
==========
    --out-path : (optional)
        Folder path to save the scraped NOMAD data (Dataset and File metadatas).
        Default is "data/nomad/{timestamp}".

Example:
========
    uv run scripts/scrap_nomad.py --out-path data/nomad/nomad_metadatas

This command will:
    1. Fetch molecular dynamics entries from the NOMAD API in batches of 50.
    2. Parse their metadata and validate them using the Pydantic models `NomadDataset`
       and `NomadFile`.
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
from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator
from tqdm import tqdm

from scripts.toolbox import format_date, validate_http_url

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


# PYDANTIC CLASS
class NomadDataset(BaseModel):
    """Class representing a Nomad molecular dynamics dataset."""

    source: str = Field(
        "NOMAD",
        description="Source of the dataset ('https://nomad-lab.eu/prod/v1/gui/').",
    )
    source_id: str = Field(
        ...,
        description="Unique identifier for the dataset in the source."
    )
    url: str = Field(
        ...,
        description="URL to access the dataset on NOMAD."
    )
    date_created: str = Field(
        ...,
        description="Date when the dataset was created."
    )
    date_last_updated: str = Field(
        ...,
        description="Date when the dataset was last updated."
    )
    date_last_crawled: str = Field(
        ...,
        description="Date when the dataset was last crawled."
    )
    title: str = Field(
        ...,
        description="Title of the dataset."
    )
    author_names: list[str] = Field(
        ...,
        description="List of author names associated with the dataset."
    )
    links: list[str] | None = Field(
        None,
        description="List of external or reference links associated with the dataset.",
    )
    license: str | None = Field(
        None,
        description="License under which the dataset is shared."
    )
    description: str | None = Field(
        None,
        description="Description or comment about the dataset."
    )

    # Files.
    nb_files: int = Field(
        ...,
        description="Number of files in the dataset."
    )
    file_names: list[str] = Field(
        ...,
        description="List of file names in the dataset."
    )

    # Simulation.
    simulation_program: str = Field(
        ...,
        description="Name of the simulation program used."
    )
    simulation_program_version: str = Field(
        ...,
        description="Version of the simulation program used."
    )
    nb_atoms: int | None = Field(
        None,
        description="Total number of atoms in the system."
    )
    molecules: list[str] | None = Field(
        None,
        description="List of (molecule_label, number_of_atoms) \
            tuples extracted fromNOMAD analysis.",
    )

    # METHODS
    @field_validator(
        "date_created", "date_last_updated", "date_last_crawled", mode="before"
    )
    def format_dates(cls, v: datetime | str) -> str:  # noqa: N805
        """Convert datetime objects or ISO strings to '%Y-%m-%dT%H:%M:%S' format.

        Parameters
        ----------
        cls : type[NomadDataset]
            The Pydantic model class being validated.
        v : str
            The input value of the 'date' field to validate.

        Returns
        -------
        str:
            The date in '%Y-%m-%dT%H:%M:%S' format.
        """
        return format_date(v)

    # To comment if u won't take time to valid all the dataset urls
    # @field_validator("url", mode="before")
    def validate_url(cls, v: str) -> str:  # noqa: N805
        """
        Validate that the URL field is a properly formatted HTTP/HTTPS URL.

        Parameters
        ----------
        cls : type[NomadDataset]
            The Pydantic model class being validated.
        v : str
            The input value of the 'url' field to validate.

        Returns
        -------
        str
            The validated URL string.
        """
        return validate_http_url(v)

    @field_validator("links", "license", "description", mode="before")
    def empty_to_none(cls, v: list | str) -> list | str | None:  # noqa: N805
        """
        Normalize empty field values by converting them to None.

        Parameters
        ----------
        cls : type[NomadDataset]
            The Pydantic model class being validated.
        v : Optional[list | str]
            The raw input value of the field before conversion.
            Can be a list, a string, or None.

        Returns
        -------
        list | str | None
            Returns None if the value is an empty list or empty string;
            otherwise returns the original value.
        """
        if v == [] or v == "":
            return None
        return v


class NomadFile(BaseModel):
    """Class representing a Nomad molecular dynamics file."""

    # --- Source (NOMAD) ---
    entry_id: str = Field(
        ...,
        description="Unique NOMAD entry identifier of the dataset related to this file."
    )
    file_url: str = Field(..., description="Full file path within the NOMAD repository")

    # --- Metadata ---
    name_file: str = Field(..., description="Name of the file in the NOMAD entry")
    type: str = Field(..., description="File extension.")
    size: int = Field(..., description="File size in bytes")
    date_last_crawled: str = Field(
        ..., description="Date when the dataset was last crawled."
    )

    # METHODS
    @field_validator("date_last_crawled", mode="before")
    def format_dates(cls, v: datetime | str) -> str:  # noqa: N805
        """Convert datetime objects or ISO strings to '%Y-%m-%dT%H:%M:%S' format.

        Parameters
        ----------
        cls : type[NomadDataset]
            The Pydantic model class being validated.
        v : str
            The input value of the 'date' field to validate.

        Returns
        -------
        str:
            The date in '%Y-%m-%dT%H:%M:%S' format.
        """
        return format_date(v)

    # To comment if u won't take time to valid all the file urls
    # @field_validator("file_url", mode="before")
    def valid_url(cls, v: str) -> str:  # noqa: N805
        """
        Validate that the URL field is a properly formatted HTTP/HTTPS URL.

        Parameters
        ----------
        cls : type[NomadFiles]
            The Pydantic model class being validated.
        v : str
            The input value of the 'url' field to validate.

        Returns
        -------
        str
            The validated URL string.
        """
        return validate_http_url(v)

    @computed_field
    @property
    def size_readable(self) -> str:
        """
        Convert the file size in bytes into a human-readable format.

        Returns
        -------
            str: The size formatted with an appropriate unit
            (B, KB, MB, GB, or TB), rounded to two decimals.
        """
        size = self.size
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        while size >= 1024 and idx < len(units) - 1:
            size /= 1024
            idx += 1
        return f"{size:.2f} {units[idx]}"


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


def test_nomad_connection() -> bool:
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
        logger.error(f"NOMAD API returned status code {r.status_code}")
        return False
    except httpx.RequestError as exc:
        logger.error(f"Failed to connect to NOMAD API: {exc}")
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
        return []

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
        while next_page_value and len(all_entries_with_time) < 10:
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


def parse_and_validate_entry_metadatas(
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> tuple[list["NomadDataset"], list[dict]]:
    """
    Parse and validate metadata fields for all NOMAD entries in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of entries and the fetch_time.

    Returns
    -------
    Tuple[List[NomadDataset], List[Dict]]
        - List of successfully validated `NomadDataset` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validation of NOMAD entries...")
    validated_entries = []
    non_validated_entry_ids = []
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
                "source": "NOMAD",
                "source_id": entry_id,
                "url": f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}",
                "links": data.get("references"),
                "title": data.get("entry_name"),
                "date_created": data.get("entry_create_time"),
                "date_last_updated": data.get("last_processing_time"),
                "date_last_crawled": fetch_time,
                "nb_files": len(data.get("files", [])),
                "file_names": data.get("files", []),
                "author_names": [a.get("name") for a in data.get("authors", [])],
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
                ),
                "nb_atoms": total_atoms,
                "molecules": molecules,
            }
            try:
                # Validate and normalize data collected wieh pydantic model
                dataset_model = NomadDataset(**parsed_entry)  # ty:ignore[invalid-argument-type]
                validated_entries.append(dataset_model)
            except ValidationError as e:
                logger.error(f"Validation failed for entry {entry_id}: {e}")
                non_validated_entry_ids.append(parsed_entry)

    logger.success(
        f"Parsing completed: {len(validated_entries)} validated / {total_entries} total\
            entries successfully! \n"
    )
    return validated_entries, non_validated_entry_ids


def parse_and_validate_files_metadatas(
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> tuple[list["NomadFile"], list[dict]]:
    """
    Parse and validate metadata fields for all NOMAD files in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of files and the fetch_time.

    Returns
    -------
    Tuple[List[NomadDataset], List[Dict]]
        - List of successfully validated `NomadFile` objects.
        - List of parsed entry that failed validation.
    """
    logger.info("Starting parsing and validation of NOMAD files...")
    validated_entries = []
    non_validated_entry_ids = []
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
                    "entry_id": entry_id,
                    "name_file": name_file,
                    "type": file_extension,
                    "size": size,
                    "file_url": file_path,
                    "date_last_crawled": fetch_time,
                }
                try:
                    # Validate and normalize data collected wieh pydantic model
                    dataset_model = NomadFile(**parsed_entry)  # ty:ignore[invalid-argument-type]
                    validated_entries.append(dataset_model)
                except ValidationError as e:
                    logger.error(f"Validation failed for file {entry_id}: {e}")
                    non_validated_entry_ids.append(parsed_entry)

    logger.success(
        f"Parsing completed: {len(validated_entries)} validated / {total_files} total \
              files successfully! \n"
    )
    return validated_entries, non_validated_entry_ids


def save_nomad_entries_metadatas_to_parquet(
    folder_out_path: Path,
    nomad_metadatas_validated: list["NomadDataset"] | list["NomadFile"],
    nomad_metadatas_unvalidated: list[dict],
    tag: str,
) -> None:
    """
    Save NOMAD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    folder_out_path : Path
        Folder path where Parquet files will be saved.
    nomad_metadatas_validated : List[NomadDataset]
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
        validated_path = os.path.join(folder_out_path, "validated_entries.parquet")
    elif tag == "files":
        validated_path = os.path.join(folder_out_path, "validated_files.parquet")
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
            folder_out_path, "unvalidated_entries.parquet"
        )
    elif tag == "files":
        unvalidated_path = os.path.join(
            folder_out_path, "unvalidated_files.parquet"
        )
    try:
        if len(nomad_metadatas_unvalidated) != 0:
            df_unvalidated = pd.DataFrame(nomad_metadatas_unvalidated)
            df_unvalidated.to_parquet(unvalidated_path, index=False)
            logger.success(
            f"NOMAD unvalidated metadatas saved to: {unvalidated_path} successfully!"
            )
        else:
            logger.warning("There is no unvalidated entries to save!")
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"Failed to save unvalidated metadata to {unvalidated_path}: {e}")


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
        # Parse and validate NOMAD entry metadatas with a pydantic model (NomadDataset)
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
        # Parse and validate the file metadatas with a pydantic model (NomadFile)
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
    # Configure logging.
    setup_logger(logger)

    # Scrap NOMAD data
    scrap_nomad_data()

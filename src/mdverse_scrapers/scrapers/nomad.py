"""Scrape molecular dynamics simulation datasets and files from NOMAD.

This script scrapes molecular dynamics datasets from the NOMAD repository
https://nomad-lab.eu/prod/v1/gui/search/entries
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import httpx
import pandas as pd
from loguru import logger
from pydantic import ValidationError

from ..core.logger import create_logger
from ..models.dataset import DatasetMetadata, DatasetProject, DatasetRepository
from ..models.file import FileMetadata

BASE_NOMAD_URL = "http://nomad-lab.eu/prod/v1/api/v1"
JSON_PAYLOAD_NOMAD_REQUEST: dict[str, Any] = {
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


def is_nomad_connection_working() -> bool | None:
    """Test connection to the NOMAD API.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to NOMAD API...")
    try:
        r = httpx.head(f"{BASE_NOMAD_URL}/entries", timeout=5)
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
        f"Fetching Molecular Dynamics related {tag} from NOMAD API "
        f"by batch of {page_size} entries..."
    )
    all_entries_with_time = []
    next_page_value = None
    total_entries = None

    # Fetch the first page
    try:
        logger.debug("Requesting first page...")
        fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # HTTP request
        json_payload = JSON_PAYLOAD_NOMAD_REQUEST
        json_payload["pagination"]["page_size"] = page_size
        json_payload["pagination"]["page_after_value"] = next_page_value
        response = httpx.post(
            f"{BASE_NOMAD_URL}/{query_entry_point}",
            json=json_payload,
            timeout=100,
        )
        response.raise_for_status()
        # Sleep briefly to avoid overwhelming the remote server
        time.sleep(0.1)
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
            f"Fetched first {len(first_entries_with_request_md['data'])}"
            f"/{total_entries} entries"
        )

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        return [([], "")]

    # Paginate through remaining entries
    logger.debug(
        f"Paginate through remaining {tag}... (usually takes around 3 minutes)"
    )
    while next_page_value:
        try:
            fetch_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            # HTTP request
            json_payload = JSON_PAYLOAD_NOMAD_REQUEST
            json_payload["pagination"]["page_size"] = page_size
            json_payload["pagination"]["page_after_value"] = next_page_value
            response = httpx.post(
                f"{BASE_NOMAD_URL}/{query_entry_point}",
                json=json_payload,
                timeout=100,
            )
            response.raise_for_status()
            # Sleep briefly to avoid overwhelming the remote server
            time.sleep(0.1)
            next_batch = response.json()
            all_entries_with_time.append((next_batch["data"], fetch_time))

            # Update the next entry to begin with
            next_page_value = next_batch.get("pagination", {}).get(
                "next_page_after_value", None
            )
            fetched_so_far = sum(len(batch) for batch, _ in all_entries_with_time)
            logger.info(f"Fetched {fetched_so_far}/{total_entries} {tag}")

        except httpx.HTTPError as e:
            logger.error(
                "HTTP error occurred while fetching next page "
                f"after {next_page_value}: {e}"
            )
            break

    total_datasets = sum(len(batch[0]) for batch in all_entries_with_time)
    total_files = sum(
        len(entry["files"]) for batch, _ in all_entries_with_time for entry in batch
    )
    logger.success(
        f"Fetched {total_datasets if tag == 'entries' else total_files} molecular "
        f"dynamics related {tag} from NOMAD successfully! \n"
    )
    return all_entries_with_time


def validate_parsed_metadatas(
    parsed: dict[str, Any], out_model: type[FileMetadata | DatasetMetadata]
) -> tuple[FileMetadata | DatasetMetadata | None, str | None]:
    """Validate a parsed entry using the pydantic model.

    Parameters
    ----------
    parsed : dict[str, Any]
        The parsed metadatas to validate.
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
            value: Any = err.get("input")

            reasons.append(f"{field}: {reason} (input={value!r})")

        non_validation_reason = "; ".join(reasons)
        return None, non_validation_reason


def parse_and_validate_entry_metadata(
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> list[DatasetMetadata]:
    """
    Parse and validate metadata fields for all NOMAD entries in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of entries and the fetch_time.

    Returns
    -------
    List[DatasetMetadata]
        List of successfully validated `DatasetMetadata` objects.
    """
    logger.info("Starting parsing and validation of NOMAD entries...")
    validated_entries = []
    total_entries = sum(len(batch) for batch, _ in nomad_data)

    for entries_list, fetch_time in nomad_data:
        for data in entries_list:
            entry_id = data.get("entry_id")
            entry_url = (
                f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}"
            )

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
                "dataset_repository_name": DatasetRepository.NOMAD,
                "dataset_project_name": DatasetProject.NOMAD,
                "dataset_id_in_repository": entry_id,
                "dataset_id_in_project": entry_id,
                "dataset_url_in_repository": entry_url,
                "dataset_url_in_project": entry_url,
                "external_links": data.get("references"),
                "title": data.get("entry_name"),
                "date_created": data.get("entry_create_time"),
                "date_last_updated": data.get("last_processing_time"),
                "date_last_fetched": fetch_time,
                "nb_files": len(data.get("files", [])),
                "author_names": [a.get("name") for a in data.get("authors", [])],
                "license": data.get("license"),
                "description": data.get("comment"),
                "software_name": (
                    data.get("results", {})
                    .get("method", {})
                    .get("simulation", {})
                    .get("program_name")
                ),
                "software_version": (
                    data.get("results", {})
                    .get("method", {})
                    .get("simulation", {})
                    .get("program_version")
                ),
                "nb_atoms": total_atoms,
                "molecule_names": molecules,
            }
            # Validate and normalize data collected with pydantic model
            (dataset_model_entry, non_validation_reason) = validate_parsed_metadatas(
                parsed_entry, DatasetMetadata
            )
            if isinstance(dataset_model_entry, DatasetMetadata):
                validated_entries.append(dataset_model_entry)
            else:
                logger.error(
                    f"Validation failed for dataset `{entry_id}` ({entry_url})"
                    ". Invalid field(s) detected : "
                    f"{non_validation_reason}"
                )

    percentage = (
        (len(validated_entries) / total_entries) * 100 if total_entries > 0 else 0.0
    )
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_entries)}/{total_entries}) datasets successfully! \n"
    )
    return validated_entries


def parse_and_validate_file_metadata(
    nomad_data: list[tuple[list[dict[str, Any]], str]],
) -> list[FileMetadata]:
    """
    Parse and validate metadata fields for all NOMAD files in batches.

    Parameters
    ----------
    nomad_data : List[Tuple[List[Dict[str, Any]], str]]
        List of tuples containing a batch of files and the fetch_time.

    Returns
    -------
    List[FileMetadata]
        List of successfully validated `FileMetadata` objects.
    """
    logger.info("Starting parsing and validation of NOMAD files...")
    validated_files = []
    total_files = sum(len(entry["files"]) for batch, _ in nomad_data for entry in batch)

    for entries_list, fetch_time in nomad_data:
        for data in entries_list:
            entry_id = data.get("entry_id")
            for file in data.get("files", []):
                name_file = file["path"].split("/")[-1]
                file_extension = name_file.split(".")[-1]
                file_path_url = (
                    f"https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/"
                    f"{entry_id}/files/{name_file}"
                )
                size = file.get("size", None)

                parsed_file = {
                    "dataset_repository_name": DatasetRepository.NOMAD,
                    "dataset_id_in_repository": entry_id,
                    "file_name": name_file,
                    "file_type": file_extension,
                    "file_size_in_bytes": size,
                    "file_url_in_repository": file_path_url,
                    "date_last_fetched": fetch_time,
                }
                # Validate and normalize data collected with pydantic model
                (
                    file_model_entry,
                    non_validation_reason,
                ) = validate_parsed_metadatas(parsed_file, FileMetadata)
                if isinstance(file_model_entry, FileMetadata):
                    validated_files.append(file_model_entry)
                else:
                    logger.error(
                        f"Validation failed for file `{name_file}` "
                        f"({file_path_url}). Invalid field(s) detected : "
                        f"{non_validation_reason}"
                    )

    percentage = (len(validated_files) / total_files) * 100 if total_files > 0 else 0.0
    logger.success(
        f"Parsing completed: {percentage:.2f}% validated "
        f"({len(validated_files)}/{total_files}) files successfully! \n"
    )
    return validated_files


def save_nomad_metadatas_to_parquet(
    output_path: Path,
    nomad_metadatas_validated: list[DatasetMetadata] | list[FileMetadata],
    tag: str,
) -> None:
    """
    Save NOMAD validated and unvalidated metadata to Parquet files.

    Parameters
    ----------
    output_path : Path
        Folder path where Parquet files will be saved.
    nomad_metadatas_validated : List[DatasetMetadata]
        List of validated NOMAD entries.
    tag: str
        Tag to know if its entries or files metadata to save.
    """
    logger.info(f"Saving NOMAD {tag} metadatas to a Parquet file...")
    # Ensure output folder exists
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # Save validated entries
    if tag == "entries":
        validated_path = os.path.join(output_path, "nomad_datasets.parquet")
    elif tag == "files":
        validated_path = os.path.join(output_path, "nomad_files.parquet")
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


@click.command()
@click.option(
    "--output-path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Directory path to save results",
)
def main(output_path: Path) -> None:
    """Scrap molecular dynamics datasets and files from NOMAD.

    Parameters
    ----------
    output_path : Path
        The output directory path for the scraped data.
    """
    output_path = output_path / "nomad"
    output_path.mkdir(parents=True, exist_ok=True)
    logger = create_logger(logpath=output_path / "nomad_scraper.log", level="INFO")
    logger.info("Starting Nomad data scraping...")
    start_time = time.perf_counter()

    if not is_nomad_connection_working:
        logger.error("Cannot scrap data, no connection to NOMAD API.")
        sys.exit(1)

    # Fetch NOMAD entries metadata
    nomad_data = fetch_nomad_md_related_by_batch(
        query_entry_point="entries/query", tag="entries"
    )
    if nomad_data == []:
        logger.warning("No data fetched from NOMAD.")
        return
    # Parse and validate NOMAD entry metadatas with a pydantic model (DatasetMetadata)
    nomad_entries_validated = parse_and_validate_entry_metadata(nomad_data)

    # Save parsed metadata to local file
    save_nomad_metadatas_to_parquet(
        output_path,
        nomad_entries_validated,
        tag="entries",
    )

    # Fetch NOMAD files metadata
    nomad_files_metadata = fetch_nomad_md_related_by_batch(
        query_entry_point="entries/rawdir/query", tag="files"
    )
    # Parse and validate the file metadatas with a pydantic model (FileMetadata)
    nomad_files_metadata_validated = parse_and_validate_file_metadata(
        nomad_files_metadata
    )
    save_nomad_metadatas_to_parquet(
        output_path,
        nomad_files_metadata_validated,
        tag="files",
    )

    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraping duration: {timedelta(seconds=elapsed_time)} 🎉")


if __name__ == "__main__":
    main()

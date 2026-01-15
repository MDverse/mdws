"""Scrape molecular dynamics simulation datasets and files from NOMAD.

This script scrapes molecular dynamics datasets from the NOMAD repository
https://nomad-lab.eu/prod/v1/gui/search/entries
"""
from importlib_metadata import files
from Bio.PDB.Dice import extract
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import httpx
import pandas as pd
import loguru
from pydantic import ValidationError

from ..core.logger import create_logger
from ..core.network import HttpMethod, create_httpx_client, make_http_request_with_retries
from ..core.toolbox import extract_file_extension, export_list_of_models_to_parquet
from ..models.dataset import DatasetMetadata
from ..models.enums import DataType, DatasetProjectName, DatasetRepositoryName
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


def is_nomad_connection_working(client: httpx.Client, url: str, logger: "loguru.Logger" = loguru.logger) -> bool | None:
    """Test connection to the NOMAD API.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to NOMAD API...")
    response = make_http_request_with_retries(client, url, method=HttpMethod.GET)
    if not response:
        logger.error("Cannot connect to the NOMAD API.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def scrape_all_datasets(client: httpx.Client,
    query_entry_point: str, page_size: int = 50,
    json_payload: dict[str, Any] | None = None,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the NOMAD API.

    Within the NOMAD terminology, datasets are referred to as "entries".
    Doc: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    query_entry_point : str
        The entry point of the API request.
    page_size : int
        Number of entries to fetch per page.

    Returns
    -------
    list[dict]:
        A list of NOMAD entries.
    """
    logger.info(
        "Scraping molecular dynamics datasets from NOMAD."
    )
    logger.info(
        f"Using batches of {page_size} datasets."
    )
    all_datasets = []
    next_page_value = None
    total_datasets = None

    # Start by requesting the first page to get total number of datasets.
    logger.info("Requesting first page to get total number of datasets...")
    # Prepare the JSON payload for the first request.
    if json_payload:
        json_payload["pagination"]["page_size"] = page_size
        json_payload["pagination"]["page_after_value"] = next_page_value
    response = make_http_request_with_retries(
        client,
        f"{BASE_NOMAD_URL}/{query_entry_point}",
        method=HttpMethod.POST,
        json=json_payload,
        timeout=60,
    )
    if not response:
        logger.critical("Failed to fetch data from NOMAD API.")
        sys.exit(1)
    try:
        # Get the formated response with request metadatas in JSON format
        response_json = response.json()
        # Get the total datasets from the request md
        total_datasets = response_json["pagination"]["total"]
        logger.info(f"Found a total of {total_datasets:,} datasets in NOMAD.")
        # Get the ID to start the next batch of datasets
        next_page_value = response_json["pagination"][
            "next_page_after_value"
        ]
        # Get only the datasets metadatas
        datasets = response_json["data"]
        # Store the first batch of datasets metadata
        all_datasets.extend(datasets)
        logger.info(
            f"Scraped first {len(response_json['data'])}"
            f"/{total_datasets} datasets"
        )
        logger.debug("First dataset metadata:")
        logger.debug(datasets[0])
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.error(f"Error while parsing NOMAD response: {exc}")
        logger.error("Cannot find datasets.")
        logger.critical("Aborting.")
        sys.exit(1)
    return all_datasets  # DEBUG
    # Paginate through remaining datasets.
    logger.info("Scraping remaining datasets")
    while next_page_value:
        if json_payload:
            json_payload["pagination"]["page_after_value"] = next_page_value
        response = make_http_request_with_retries(
            client,
            f"{BASE_NOMAD_URL}/{query_entry_point}",
            method=HttpMethod.POST,
            json=json_payload,
            timeout=60,
        )
        if not response:
            logger.error("Failed to fetch data from NOMAD API.")
            logger.error("Jumping to next iteration.")
            continue
        try:
            response_json = response.json()
            entries = response_json["data"]
            # Update the next page to start with.
            next_page_value = response_json.get("pagination", {}).get(
                "next_page_after_value", None
            )
        except (json.decoder.JSONDecodeError, ValueError) as exc:
            logger.error(f"Error while parsing NOMAD response: {exc}")
            logger.error("Jumping to next iteration.")
        logger.info(f"Found {len(datasets)} datasets in this batch.")
        all_datasets += datasets
        logger.info(
            f"Scraped {len(all_datasets)}/{total_datasets:,} "
            f"({len(all_datasets) / total_datasets:.0%}) datasets."
        )
    logger.success(
        f"Scraped {len(all_datasets)} datasets in NOMAD."
    )
    return all_datasets


def scrape_files_metadata_for_dataset(
        client: httpx.Client,
        url: str,
        dataset_id: str,
        logger: "loguru.Logger" = loguru.logger
    ) -> dict | None:
    """
    Scrape files metadata for a given NOMAD dataset.

    Doc: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    dataset_id : str
        The unique identifier of the dataset in NOMAD.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    dict | None
        File metadata dictionary for the dataset.
    """
    logger.info(f"Scraping files for dataset ID: {dataset_id}")
    response = make_http_request_with_retries(
        client,
        url,
        method=HttpMethod.GET,
        timeout=30,
    )
    if not response:
        logger.error("Failed to fetch files metadata.")
        return None
    return response.json()


def validate_metadata(
        metadata: dict[str, Any],
        model: type[FileMetadata | DatasetMetadata],
        logger: "loguru.Logger" = loguru.logger,
    ) -> FileMetadata | DatasetMetadata | None:
    """Validate metadata against a Pydantic model.

    Parameters
    ----------
    metadata: dict[str, Any]
        The metadatas to validate.
    model: type[FileMetadata | DatasetMetadata]
        The Pydantic model used for the validation.

    Returns
    -------
    type[FileMetadata | DatasetMetadata] | None
        Validated model instance or None if validation fails.
    """
    try:
        return model(**metadata)
    except ValidationError as exc:
        logger.warning("Validation error!")
        for error in exc.errors():
            field = error["loc"]
            logger.debug(f"Field: {field[0]}")
            if len(field) > 1:
                logger.debug(f"Subfield: {field[1]}")
            logger.debug(f"Error type: {error.get('input')}")
            logger.debug(f"Reason: {error['msg']}")
            inputs = error["input"]
            if not isinstance(inputs, dict):
                logger.debug(f"Input value: {inputs}")
            else:
                logger.debug("Input is a complex structure. Skipping value display.")
        return None


def extract_datasets_metadata(datasets: list[dict[str, Any]],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw NOMAD datasets metadata.

    Parameters
    ----------
    datasets : List[Dict[str, Any]]
        List of raw NOMAD datasets metadata.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset in datasets:
        entry_id = dataset.get("entry_id")
        logger.info(f"Extracting relevant metadata for dataset: {entry_id}")
        entry_url = (
            f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}"
        )
        metadata = {
            "dataset_repository_name": DatasetRepositoryName.NOMAD,
            "dataset_project_name": DatasetProjectName.NOMAD,
            "dataset_id_in_repository": entry_id,
            "dataset_id_in_project": entry_id,
            "dataset_url_in_repository": entry_url,
            "dataset_url_in_project": entry_url,
            "external_links": dataset.get("references"),
            "title": dataset.get("entry_name"),
            "date_created": dataset.get("entry_create_time"),
            "date_last_updated": dataset.get("last_processing_time"),
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "nb_files": len(dataset.get("files", [])),
            "author_names": [a.get("name") for a in dataset.get("authors", [])],
            "license": dataset.get("license"),
            "description": dataset.get("comment")
        }
        # Extract simulation metadata if available.
        # Software name.
        software_name = None
        try:
            software_name = (
                dataset.get("results", {})
                .get("method", {})
                .get("simulation", {})
                .get("program_name")
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing software name for entry {entry_id}: {e}")
        metadata["software_name"] = software_name
        # Software version
        software_version = None
        try:
            software_version = (
                dataset.get("results", {})
                .get("method", {})
                .get("simulation", {})
                .get("program_version")
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing software version for entry {entry_id}: {e}")
        metadata["software_version"] = software_version
        # Molecules and number total of atoms.
        total_atoms = None
        molecules = None
        try:
            topology = (
                dataset.get("results", {}).get("material", {}).get("topology", [])
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
        metadata["nb_atoms"] = total_atoms
        metadata["molecule_names"] = molecules
        datasets_metadata.append(metadata)
    logger.info(f"Extracted metadata for {len(datasets_metadata)} datasets.")
    return datasets_metadata


def normalize_datasets_metadata(
    datasets: list[dict],
    logger: "loguru.Logger" = loguru.logger,
) -> list[DatasetMetadata]:
    """
    Normalize dataset metadata with a Pydantic model.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dictionaries.

    Returns
    -------
    list[DatasetMetadata]
        List of successfully validated `DatasetMetadata` objects.
    """
    datasets_metadata = []
    for dataset in datasets:
        logger.info(
            "Normalizing metadata for dataset: "
            f"{dataset['dataset_id_in_repository']}"
        )
        normalized_metadata = validate_metadata(dataset, DatasetMetadata, logger=logger)
        if not normalized_metadata:
            logger.error(
                f"Normalization failed for metadata of dataset "
                f"{dataset['dataset_id_in_repository']}"
            )
            continue
        datasets_metadata.append(normalized_metadata)
    logger.info(
        "Normalized metadata for "
        f"{len(datasets_metadata)}/{len(datasets)} "
        f"({len(datasets_metadata) / len(datasets):.0%}) datasets."
    )
    return datasets_metadata


def extract_files_metadata(
    raw_metadata: dict,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw NOMAD files metadata.

    Parameters
    ----------
    raw_metadata: dict
        Raw files metadata.

    Returns
    -------
    list[dict]
        List of select files metadata.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    entry_id = raw_metadata["entry_id"]
    for nomad_file in raw_metadata.get("data", {}).get("files", []):
        file_path = Path(nomad_file.get("path", ""))
        file_name = file_path.name
        file_type = file_path.suffix.lstrip(".")
        file_path_url = (
            f"https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/"
            f"{entry_id}/files/{file_name}"
        )
        size = nomad_file.get("size", None)

        parsed_file = {
            "dataset_repository_name": DatasetRepositoryName.NOMAD,
            "dataset_id_in_repository": entry_id,
            "file_name": file_name,
            "file_type": file_type,
            "file_size_in_bytes": size,
            "file_url_in_repository": file_path_url,
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
        files_metadata.append(parsed_file)
    logger.info(f"Extracted metadata for {len(files_metadata)} files.")
    return files_metadata


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
    # Create directories and logger.
    output_path = output_path / DatasetProjectName.NOMAD.value
    output_path.mkdir(parents=True, exist_ok=True)
    logfile_path = output_path / f"{DatasetProjectName.NOMAD.value}_scraper.log"
    logger = create_logger(logpath=logfile_path, level="INFO")
    logger.info("Starting Nomad data scraping...")
    start_time = time.perf_counter()
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to NOMAD API
    if is_nomad_connection_working(client, f"{BASE_NOMAD_URL}/entries"):
        logger.success("Connection to NOMAD API successful!")
    else:
        logger.critical("Connection to NOMAD API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape NOMAD datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client,
        query_entry_point="entries/query",
        json_payload=JSON_PAYLOAD_NOMAD_REQUEST,
        logger=logger,
    )
    if not datasets_raw_metadata:
        logger.critical("No datasets found in NOMAD.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Select datasets metadata
    datasets_selected_metadata = extract_datasets_metadata(datasets_raw_metadata, logger=logger)
    # Parse and validate NOMAD dataset metadata with a pydantic model (DatasetMetadata)
    datasets_normalized_metadata = normalize_datasets_metadata(datasets_selected_metadata)
    # Save datasets metadata to parquet file.
    export_list_of_models_to_parquet(
        output_path / f"{DatasetProjectName.NOMAD.value}_{DataType.DATASETS.value}.parquet",
        datasets_normalized_metadata,
        logger=logger
    )
    # Scrape NOMAD files metadata.
    files_normalized_metadata = []
    for dataset_count, dataset in enumerate(datasets_normalized_metadata, start=1):
        dataset_id = dataset.dataset_id_in_repository
        files_metadata = scrape_files_metadata_for_dataset(
            client,
            url=f"{BASE_NOMAD_URL}/entries/{dataset_id}/rawdir",
            dataset_id=dataset_id,
            logger=logger,
        )
        # Extract relevant files metadata.
        files_selected_metadata = extract_files_metadata(
            files_metadata,
            logger=logger
        )
        # Normalize files metadata with pydantic model (FileMetadata)
        for file_metadata in files_selected_metadata:
            normalized_metadata = validate_metadata(
                file_metadata,
                FileMetadata,
                logger=logger,
            )
            if not normalized_metadata:
                logger.error(
                    f"Normalization failed for metadata of file "
                    f"{file_metadata.get('file_name')} "
                    f"in dataset {dataset_id}"
                )
                continue
            files_normalized_metadata.append(normalized_metadata)
        logger.info(f"Validated files metadata for dataset: {dataset_id}")
        logger.info(f"Total files: {len(files_normalized_metadata)}")
        logger.info(
            "Extracted and validated files metadata for "
            f"{dataset_count}/{len(datasets_normalized_metadata)} "
            f"({dataset_count / len(datasets_normalized_metadata):.0%}) datasets."
        )
    # Save files metadata to parquet file.
    export_list_of_models_to_parquet(
        output_path / f"{DatasetProjectName.NOMAD.value}_{DataType.FILES.value}.parquet",
        files_normalized_metadata,
        logger=logger
    )

    # Print script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraped NOMAD in: {timedelta(seconds=elapsed_time)} 🎉")


if __name__ == "__main__":
    main()

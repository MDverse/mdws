"""Scrape molecular dynamics simulation datasets and files from MDposit.

This script scrapes molecular dynamics datasets from the MDposit repository
https://mmb-dev.mddbr.eu/#/browse
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import httpx
import loguru

from ..core.logger import create_logger
from ..core.network import (
    HttpMethod,
    create_httpx_client,
    make_http_request_with_retries,
)
from ..core.toolbox import export_list_of_models_to_parquet
from ..models.dataset import DatasetMetadata
from ..models.enums import DatasetProjectName, DatasetRepositoryName, DataType
from ..models.file import FileMetadata
from ..models.utils import validate_metadata_against_model

BASE_MDPOSIT_URL = "https://mmb-dev.mddbr.eu/api/rest/v1"


def is_mdposit_connection_working(
    client: httpx.Client, url: str, logger: "loguru.Logger" = loguru.logger
) -> bool | None:
    """Test connection to the MDposit API.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to MDposit API...")
    response = make_http_request_with_retries(client, url, method=HttpMethod.GET)
    if not response:
        logger.error("Cannot connect to the MDposit API.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    page_size: int = 50,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the MDposit API.

    Within the MDposit terminology, datasets are referred to as "projects".

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    query_entry_point : str
        The entry point of the API request.
    page_size : int
        Number of entries to fetch per page.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]:
        A list of MDposit entries.
    """
    logger.info("Scraping molecular dynamics datasets from MDposit.")
    logger.info(f"Using batches of {page_size} datasets.")
    all_datasets = []

    # Start by requesting the first page to get total number of datasets.
    logger.info("Requesting first page to get total number of datasets...")
    page = 0  # start with first page

    while True:
        response = make_http_request_with_retries(
            client,
            f"{BASE_MDPOSIT_URL}/{query_entry_point}?limit={page_size}&page={page}",
            method=HttpMethod.GET,
            timeout=60,
            delay_before_request=0.2,
        )

        if not response:
            logger.error("Failed to fetch data from MDposit API.")
            logger.error("Jumping to next iteration.")
            page += 1
            continue

        try:
            response_json = response.json()
            datasets = response_json.get("projects", [])
            total_datasets = response_json.get("filteredCount")

            if page == 0 and total_datasets is not None:
                logger.info(f"Found a total of {total_datasets:,} datasets in MDposit.")

            if not datasets:
                logger.info("No more datasets returned by API. Stopping pagination.")
                break

            all_datasets.extend(datasets)

            logger.info(f"Scraped page {page} with {len(datasets)} datasets.")
            if total_datasets:
                logger.info(
                    f"Scraped {len(all_datasets)} datasets "
                    f"({len(all_datasets):,}/{total_datasets:,} "
                    f"{len(all_datasets) / total_datasets:.0%})"
                )
            logger.debug("First dataset metadata on this page:")
            logger.debug(datasets[0] if datasets else "No datasets on this page")

        except (json.decoder.JSONDecodeError, ValueError) as exc:
            logger.error(f"Error while parsing MDposit response: {exc}")
            logger.error("Jumping to next iteration.")

        page += 1  # increment page for next iteration

    logger.success(f"Scraped {len(all_datasets)} datasets in MDposit.")
    return all_datasets


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """Scrape files metadata for all datasets in MDposit.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : list[DatasetMetadata]
        List of datasets to scrape files metadata for.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[FileMetadata]
        List of successfully validated `FileMetadata` objects.
    """
    all_files_metadata = []
    for dataset_count, dataset in enumerate(datasets, start=1):
        dataset_id = dataset.dataset_id_in_repository
        files_metadata = scrape_files_for_one_dataset(
            client,
            url=f"{BASE_MDPOSIT_URL}/projects/{dataset_id}/filenotes",
            dataset_id=dataset_id,
            logger=logger,
        )
        if not files_metadata:
            continue
        # Extract relevant files metadata.
        files_selected_metadata = extract_files_metadata(files_metadata, dataset_id, logger=logger)
        # Normalize files metadata with pydantic model (FileMetadata)
        logger.info(f"Validating files metadata for dataset: {dataset_id}")
        for file_metadata in files_selected_metadata:
            normalized_metadata = validate_metadata_against_model(
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
            all_files_metadata.append(normalized_metadata)
        logger.info("Done.")
        logger.info(f"Total files: {len(all_files_metadata):,}")
        logger.info(
            "Extracted and validated files metadata for "
            f"{dataset_count:,}/{len(datasets):,} "
            f"({dataset_count / len(datasets):.0%}) datasets."
        )
    return all_files_metadata


def scrape_files_for_one_dataset(
    client: httpx.Client,
    url: str,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """
    Scrape files metadata for a given MDposit dataset.

    Doc: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    dataset_id : str
        The unique identifier of the dataset in MDposit.
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
        timeout=60,
        delay_before_request=0.1,
    )
    if not response:
        logger.error("Failed to fetch files metadata.")
        return None
    return response.json()


def extract_datasets_metadata(
    datasets: list[dict[str, Any]],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw MDposit datasets metadata.

    Parameters
    ----------
    datasets : List[Dict[str, Any]]
        List of raw MDposit datasets metadata.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset in datasets:
        dataset_id = dataset.get("accession")
        logger.info(f"Extracting relevant metadata for dataset: {dataset_id}")
        entry_url = (
            f"https://mmb-dev.mddbr.eu/#/id/{dataset_id}/overview"
        )
        dataset_metadata = dataset.get("metadata", {})
        links = dataset_metadata.get("CITATION")
        links_list = [links] if links else None
        a = dataset_metadata.get("AUTHORS")
        author_names = a if isinstance(a, list) else [a] if a else None
        metadata = {
            "dataset_repository_name": DatasetRepositoryName.MDPOSIT,
            "dataset_project_name": DatasetProjectName.MDDB,
            "dataset_id_in_repository": dataset_id,
            "dataset_id_in_project": dataset_id,  # idk? Maybe None
            "dataset_url_in_repository": entry_url,
            "dataset_url_in_project": entry_url,  # idk? Maybe None
            "external_links": links_list,
            "title": dataset_metadata.get("NAME"),
            "date_created": dataset.get("creationDate"),
            "date_last_updated": dataset.get("updateDate"),
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "nb_files": len(dataset.get("files", [])),
            "author_names": author_names,
            "license": dataset_metadata.get("LICENSE"),
            "description": dataset_metadata.get("DESCRIPTION"),
            "software_name": dataset_metadata.get("PROGRAM"),
            "software_version": str(dataset_metadata.get("VERSION")),
            "nb_atoms": dataset_metadata.get("atomCount"),
            "forcefield_model_name":  ", ".join(
                        filter(None, dataset_metadata.get("FF") or [])),
            "simulation_temperature": [str(dataset_metadata.get("TEMP"))],
            "molecule_names": dataset_metadata.get("SEQUENCES"),
        }
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
            f"Normalizing metadata for dataset: {dataset['dataset_id_in_repository']}"
        )
        normalized_metadata = validate_metadata_against_model(
            dataset, DatasetMetadata, logger=logger
        )
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
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw MDposit files metadata.

    Parameters
    ----------
    raw_metadata: dict
        Raw files metadata.
    dataset_id : str
        The unique identifier of the dataset in MDposit.

    Returns
    -------
    list[dict]
        List of select files metadata.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    for mdposit_file in raw_metadata:
        file_name = Path(mdposit_file.get("filename"))
        file_type = file_name.suffix.lstrip(".")
        file_path_url = (
            f"https://mmb-dev.mddbr.eu/api/rest/current/projects/{dataset_id}/files/{file_name}")

        parsed_file = {
            "dataset_repository_name": DatasetRepositoryName.MDPOSIT,
            "dataset_id_in_repository": dataset_id,
            "file_name": str(file_name),
            "file_type": file_type,
            "file_size_in_bytes": mdposit_file.get("length", None),
            "file_md5": mdposit_file.get("md5", None),
            "file_url_in_repository": file_path_url,
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
        files_metadata.append(parsed_file)
    logger.info(f"Extracted metadata for {len(files_metadata)} files.")
    return files_metadata


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
def main(output_dir_path: Path) -> None:
    """Scrape molecular dynamics datasets and files from MDposit."""
    # Create directories and logger.
    output_dir_path = output_dir_path / DatasetProjectName.MDDB.value
    output_dir_path.mkdir(parents=True, exist_ok=True)
    logfile_path = output_dir_path / f"{DatasetProjectName.MDDB.value}_scraper.log"
    logger = create_logger(logpath=logfile_path, level="INFO")
    logger.info("Starting MDposit data scraping...")
    start_time = time.perf_counter()
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to MDposit API
    if is_mdposit_connection_working(client, f"{BASE_MDPOSIT_URL}/projects/summary"):
        logger.success("Connection to MDposit API successful!")
    else:
        logger.critical("Connection to MDposit API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape MDposit datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client,
        query_entry_point="/projects",
        logger=logger,
    )
    if not datasets_raw_metadata:
        logger.critical("No datasets found in MDposit.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Select datasets metadata
    datasets_selected_metadata = extract_datasets_metadata(
        datasets_raw_metadata, logger=logger
    )
    # Parse and validate MDposit dataset metadata with a pydantic model (DatasetMetadata)
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata, logger=logger
    )
    # Save datasets metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetProjectName.MDDB.value}_{DataType.DATASETS.value}.parquet",
        datasets_normalized_metadata,
        logger=logger,
    )
    # Scrape MDposit files metadata.
    files_normalized_metadata = scrape_files_for_all_datasets(
        client, datasets_normalized_metadata, logger=logger
    )

    # Save files metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetProjectName.MDDB.value}_{DataType.FILES.value}.parquet",
        files_normalized_metadata,
        logger=logger,
    )

    # Print script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraped MDposit in: {timedelta(seconds=elapsed_time)} 🎉")


if __name__ == "__main__":
    main()

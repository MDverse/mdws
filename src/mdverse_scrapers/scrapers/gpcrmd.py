"""Scrape molecular dynamics simulation datasets and files from GPCRmd.

This script scrapes molecular dynamics datasets from the GPCRmd repository
(https://www.gpcrmd.org/), a platform dedicated to simulations of
G-protein-coupled receptors (GPCRs), a major family of membrane proteins and
frequent drug targets.
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
from bs4 import BeautifulSoup, Tag

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

BASE_GPCRMD_URL = "https://www.gpcrmd.org/api/search_all/info/"


def is_gpcrmd_connection_working(
    client: httpx.Client, url: str, logger: "loguru.Logger" = loguru.logger
) -> bool | None:
    """Test connection to the GPCRmd API.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to GPCRmd API...")
    response = make_http_request_with_retries(client, url, method=HttpMethod.GET)
    if not response:
        logger.error("Cannot connect to the GPCRmd API.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the GPCRmd API.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    query_entry_point : str
        The entry point of the API request.

    Returns
    -------
    list[dict]:
        A list of GPCRmd entries.
    """
    logger.info("Scraping molecular dynamics datasets from GPCRmd.")
    logger.info("Requesting all datasets in a single fetch...")

    response = make_http_request_with_retries(
        client,
        query_entry_point,
        method=HttpMethod.GET,
        timeout=60,
        delay_before_request=0.2,
    )
    if not response:
        logger.critical("Failed to fetch data from GPCRmd API.")
        sys.exit(1)
    try:
        # Get the formatted response with request metadatas in JSON format
        all_datasets = response.json()
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.error(f"Error while parsing GPCRmd response: {exc}")
        logger.error("Cannot find datasets.")
        logger.critical("Aborting.")
        sys.exit(1)

    logger.success(f"Scraped {len(all_datasets)} datasets in GPCRmd.")
    return all_datasets


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """Scrape files metadata for all datasets in GPCRmd.

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
            url=f"{BASE_GPCRMD_URL}/entries/{dataset_id}/rawdir",
            dataset_id=dataset_id,
            logger=logger,
        )
        if not files_metadata:
            continue
        # Extract relevant files metadata.
        files_selected_metadata = extract_files_metadata(files_metadata, logger=logger)
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
    Scrape files metadata for a given GPCRmd dataset.

    Doc: https://gpcrmd-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    dataset_id : str
        The unique identifier of the dataset in GPCRmd.
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


def fetch_dataset_page(url: str | None,
    dataset_id: str | None,
    client: httpx.Client,
    logger: "loguru.Logger" = loguru.logger
) -> str | None:
    """Fetch an dataset page and return its HTML content.

    Parameters
    ----------
    url : str
        The URL of the dataset page to fetch.
    client : httpx.Client
        The HTTPX client to use for making requests.

    Returns
    -------
    str | None
        The HTML content of the page if the request is successful, otherwise None.
    """
    if url:
        html_header = make_http_request_with_retries(
            client,
            url,
            method=HttpMethod.GET,
            timeout=60,
            delay_before_request=0.2,
        )
        if html_header:
            html_content = html_header.text

    return html_content


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
    soup = BeautifulSoup(html, "html.parser")
    bold_tag = next(
        (b for b in soup.find_all("b") if b.get_text(strip=True) == field_name),
        None,
    )
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


def extract_datasets_metadata(
    datasets: list[dict[str, Any]],
    client: httpx.Client,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw GPCRmd datasets metadata.

    Parameters
    ----------
    datasets : List[Dict[str, Any]]
        List of raw GPCRmd datasets metadata.
    client : httpx.Client
        The HTTPX client to use for making requests.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset in datasets:
        dataset_id = str(dataset.get("dyn_id"))
        logger.info(f"Extracting relevant metadata for dataset: {dataset_id}")
        dataset_url = dataset.get("url")
        metadata = {
            "dataset_repository_name": DatasetRepositoryName.GPCRMD,
            "dataset_project_name": DatasetProjectName.GPCRMD,
            "dataset_id_in_repository": dataset_id,
            "dataset_id_in_project": dataset_id,
            "dataset_url_in_repository": dataset_url,
            "dataset_url_in_project": dataset_url,
            "title": dataset.get("modelname"),
            "date_created": dataset.get("creation_timestamp"),
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "simulation_program_name": dataset.get("mysoftware"),
            "simulation_program_version": dataset.get("software_version"),
            "forcefield_model_name": dataset.get("forcefield"),
            "forcefield_model_version": dataset.get("forcefield_version"),
            "timestep": dataset.get("timestep"),
            "delta": dataset.get("delta"),
            "nb_atoms": dataset.get("atom_num")
        }
        # Extract other metadata from dataset url page if available.
        # Fetch dataset page with url
        html_content = fetch_dataset_page(dataset_url, dataset_id, client, logger)
        if html_content is None:
            logger.warning("Error parsing additionnal metadatas from web page for entry"
                           f" {dataset_id} ({dataset_url})")
            logger.warning("Skipping this step.")
            continue

        # Author names.
        author_names = None
        try:
            author_names = retrieve_metadata(html_content, "Submitted by")
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing software name for entry {dataset_id}: {e}")
        metadata["author_names"] = (author_names if author_names
                                    is None else [author_names])

        """# Software version
        software_version = None
        try:
            software_version = (
                dataset.get("results", {})
                .get("method", {})
                .get("simulation", {})
                .get("program_version")
            )
        except (ValueError, KeyError) as e:
            logger.warning(
                f"Error parsing software version for entry {dataset_id}: {e}")
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
            logger.warning(f"Error parsing molecules for entry {dataset_id}: {e}")
        metadata["nb_atoms"] = total_atoms
        metadata["molecule_names"] = molecules
        datasets_metadata.append(metadata)"""
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
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw GPCRmd files metadata.

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
            f"https://gpcrmd-lab.eu/prod/v1/gui/search/entries/entry/id/"
            f"{entry_id}/files/{file_name}"
        )
        size = nomad_file.get("size", None)

        parsed_file = {
            "dataset_repository_name": DatasetRepositoryName.GPCRMD,
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
    """Scrape molecular dynamics datasets and files from GPCRmd."""
    # Create directories and logger.
    output_dir_path = output_dir_path / DatasetProjectName.GPCRMD.value
    output_dir_path.mkdir(parents=True, exist_ok=True)
    logfile_path = output_dir_path / f"{DatasetProjectName.GPCRMD.value}_scraper.log"
    logger = create_logger(logpath=logfile_path, level="INFO")
    logger.info("Starting GPCRmd data scraping...")
    start_time = time.perf_counter()
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to GPCRmd API
    if is_gpcrmd_connection_working(client, f"{BASE_GPCRMD_URL}"):
        logger.success("Connection to GPCRmd API successful!")
    else:
        logger.critical("Connection to GPCRmd API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape GPCRmd datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client,
        query_entry_point="entries/query",
        logger=logger,
    )
    if not datasets_raw_metadata:
        logger.critical("No datasets found in GPCRmd.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Select datasets metadata
    datasets_selected_metadata = extract_datasets_metadata(
        datasets_raw_metadata, client, logger=logger
    )
    """
    # Parse and validate GPCRmd dataset metadata with a pydantic model (DatasetMetadata)
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata,
        logger=logger
    )
    # Save datasets metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetProjectName.GPCRMD.value}_{DataType.DATASETS.value}.parquet",
        datasets_normalized_metadata,
        logger=logger,
    )
    # Scrape GPCRmd files metadata.
    files_normalized_metadata = scrape_files_for_all_datasets(
        client, datasets_normalized_metadata, logger=logger
    )

    # Save files metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetProjectName.GPCRMD.value}_{DataType.FILES.value}.parquet",
        files_normalized_metadata,
        logger=logger,
    )

    # Print script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraped GPCRmd in: {timedelta(seconds=elapsed_time)} 🎉")
    """


if __name__ == "__main__":
    main()

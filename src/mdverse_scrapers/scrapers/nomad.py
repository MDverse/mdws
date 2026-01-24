"""Scrape molecular dynamics simulation datasets and files from NOMAD.

This script scrapes molecular dynamics datasets from the NOMAD repository
https://nomad-lab.eu/prod/v1/gui/search/entries
"""

import json
import sys
import time
from datetime import timedelta
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
from ..models.enums import DatasetSourceName, DataType
from ..models.file import FileMetadata
from ..models.simulation import Molecule, Software
from ..models.utils import validate_metadata_against_model

BASE_NOMAD_URL = "http://nomad-lab.eu/prod/v1/api/v1"
JSON_PAYLOAD_NOMAD_REQUEST: dict[str, Any] = {
    "owner": "visible",
    "query": {"results.method.workflow_name:any": ["MolecularDynamics"]},
    "aggregations": {},
    "pagination": {"order_by": "upload_create_time", "order": "desc", "page_size": 10},
    "required": {"exclude": ["quantities", "sections"]},
}


def is_nomad_connection_working(
    client: httpx.Client, url: str, logger: "loguru.Logger" = loguru.logger
) -> bool | None:
    """Test connection to the NOMAD API.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    logger: "loguru.Logger"
        Logger object.

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


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    page_size: int = 50,
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
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[dict]:
        A list of NOMAD entries.
    """
    logger.info("Scraping molecular dynamics datasets from NOMAD.")
    logger.info(f"Using batches of {page_size} datasets.")
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
        delay_before_request=0.2,
    )
    if not response:
        logger.critical("Failed to fetch data from NOMAD API.")
        sys.exit(1)
    try:
        # Get the formatted response with request metadatas in JSON format
        response_json = response.json()
        # Get the total datasets from the request md
        total_datasets = response_json["pagination"]["total"]
        logger.info(f"Found a total of {total_datasets:,} datasets in NOMAD.")
        # Get the ID to start the next batch of datasets
        next_page_value = response_json["pagination"]["next_page_after_value"]
        # Get only the datasets metadatas
        datasets = response_json["data"]
        # Store the first batch of datasets metadata
        all_datasets.extend(datasets)
        logger.info(
            f"Scraped first {len(response_json['data'])}/{total_datasets} datasets"
        )
        logger.debug("First dataset metadata:")
        logger.debug(datasets[0])
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.error(f"Error while parsing NOMAD response: {exc}")
        logger.error("Cannot find datasets.")
        logger.critical("Aborting.")
        sys.exit(1)

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
            delay_before_request=0.2,
        )
        if not response:
            logger.error("Failed to fetch data from NOMAD API.")
            logger.error("Jumping to next iteration.")
            continue
        try:
            response_json = response.json()
            datasets = response_json["data"]
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
            f"Scraped {len(all_datasets)} datasets "
            f"({len(all_datasets):,}/{total_datasets:,}"
            f":{len(all_datasets) / total_datasets:.0%})"
        )
    logger.success(f"Scraped {len(all_datasets)} datasets in NOMAD.")
    return all_datasets


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """Scrape files metadata for all datasets in NOMAD.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : list[DatasetMetadata]
        List of datasets to scrape files metadata for.
    logger: "loguru.Logger"
        Logger object.

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
            url=f"{BASE_NOMAD_URL}/entries/{dataset_id}/rawdir",
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
        logger.info(f"Total files found: {len(all_files_metadata):,}")
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
        Logger object.

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


def extract_software_and_version(
    dataset: dict, entry_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[Software] | None:
    """
    Extract software name and version from the nested dataset dictionary.

    Parameters
    ----------
    dataset : dict
        The dataset entry from which to extract software information.
    entry_id : str
        Identifier of the dataset entry, used for logging.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[Software] | None
        A list of Software instances with `name` and `version` fields, None otherwise.
    """
    name = None
    version = None
    try:
        software_info = (
            dataset.get("results", {}).get("method", {}).get("simulation", {})
        )
        name = software_info.get("program_name")
        version = software_info.get("program_version")
        return [Software(name=name, version=version)]
    except (ValueError, KeyError) as e:
        logger.warning(f"Error parsing software info for entry {entry_id}: {e}")
    return None


def extract_molecules_and_total_atoms(
    dataset: dict, entry_id: str, logger: "loguru.Logger" = loguru.logger
) -> tuple[int | None, list[Molecule]]:
    """
    Extract molecules and total number of atoms from a dataset entry.

    Parameters
    ----------
    dataset : dict
        Dataset metadata obtained from the NOMAD API.
    entry_id : str
        Identifier of the dataset entry, used for logging.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    tuple[int | None, list[Molecule]]
        total_atoms: Number of atoms for the "original" label, or None if not found.
        molecules: List of Molecule objects extracted from the topology.
    """
    total_atoms = None
    molecules = []

    try:
        topologies = dataset.get("results", {}).get("material", {}).get("topology", [])
        if isinstance(topologies, list):
            # Extract total_atoms from the topology labeled "original".
            for topology in topologies:
                if topology.get("label") == "original":
                    total_atoms = topology.get("n_atoms")
                    break
            # Extract molecules
            for topology in topologies:
                if topology.get("structural_type") == "molecule":
                    molecules.append(  # noqa: PERF401
                        Molecule(
                            name=topology.get("label", "unknown"),
                            number_of_atoms=topology.get("n_atoms"),
                            formula=topology.get("chemical_formula_descriptive"),
                        )
                    )
        else:
            logger.warning(f"Topologies is not a list for entry {entry_id}.")
            logger.warning("Skipping molecules extraction.")
    except (ValueError, KeyError) as e:
        logger.warning(f"Error parsing molecules for entry {entry_id}: {e}")

    return total_atoms, molecules


def extract_time_step(
    dataset: dict, entry_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[float] | None:
    """
    Extract the simulation time step from a dataset entry.

    Convert the time step from seconds to femtoseconds.

    Parameters
    ----------
    dataset : dict
        The dataset entry containing the thermodynamic/trajectory information.
    entry_id : str
        Identifier of the dataset entry, used for logging.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[float] | None
        A list containined the time step in fs, or None if not found.
    """
    time_step = None
    try:
        time_step = (
            dataset.get("results", {})
            .get("properties", {})
            .get("thermodynamic", {})
            .get("trajectory", [{}])[0]
            .get("provenance", {})
            .get("molecular_dynamics", {})
            .get("time_step")
        )
        time_step = float(time_step) * 1e15 if time_step is not None else None
    except (ValueError, KeyError, IndexError) as e:
        logger.warning(f"Could not extract time step for entry {entry_id}: {e}")
    if time_step is None:
        return None
    return [time_step]


def extract_datasets_metadata(
    datasets: list[dict],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw NOMAD datasets metadata.

    Parameters
    ----------
    datasets : list[dict]
        List of raw NOMAD datasets metadata.
    logger: "loguru.Logger"
        Logger object.

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
            "dataset_repository_name": DatasetSourceName.NOMAD,
            "dataset_id_in_repository": entry_id,
            "dataset_url_in_repository": entry_url,
            "external_links": dataset.get("references"),
            "title": dataset.get("entry_name"),
            "date_created": dataset.get("entry_create_time"),
            "date_last_updated": dataset.get("last_processing_time"),
            "number_of_files": len(dataset.get("files", [])),
            "author_names": [a.get("name") for a in dataset.get("authors", [])],
            "license": dataset.get("license"),
            "description": dataset.get("comment"),
        }
        # Extract simulation metadata if available.
        # Software names with their versions.
        metadata["software"] = extract_software_and_version(dataset, entry_id, logger)
        # Molecules with their nb of atoms and number total of atoms.
        total_atoms, molecules = extract_molecules_and_total_atoms(
            dataset, entry_id, logger
        )
        metadata["total_number_of_atoms"] = total_atoms
        metadata["molecules"] = molecules
        # Time step in fs.
        metadata["simulation_timesteps_in_fs"] = extract_time_step(
            dataset, entry_id, logger
        )
        # Temperatures.
        metadata["simulation_temperatures_in_kelvin"] = None  # TODO?

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
    logger: "loguru.Logger"
        Logger object.

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
    Extract relevant metadata from raw NOMAD files metadata.

    Parameters
    ----------
    raw_metadata: dict
        Raw files metadata.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[dict]
        List of select files metadata.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    entry_id = raw_metadata["entry_id"]
    entry_url = f"https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id={entry_id}"
    for nomad_file in raw_metadata.get("data", {}).get("files", []):
        file_path = Path(nomad_file.get("path", ""))
        file_name = file_path.name
        file_path_url = (
            f"https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/"
            f"{entry_id}/files/{file_name}"
        )
        size = nomad_file.get("size", None)

        parsed_file = {
            "dataset_repository_name": DatasetSourceName.NOMAD,
            "dataset_id_in_repository": entry_id,
            "dataset_url_in_repository": entry_url,
            "file_name": file_name,
            "file_size_in_bytes": size,
            "file_url_in_repository": file_path_url,
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
    """Scrape molecular dynamics datasets and files from NOMAD."""
    # Create directories and logger.
    output_dir_path = output_dir_path / DatasetSourceName.NOMAD.value
    output_dir_path.mkdir(parents=True, exist_ok=True)
    logfile_path = output_dir_path / f"{DatasetSourceName.NOMAD.value}_scraper.log"
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
    datasets_selected_metadata = extract_datasets_metadata(
        datasets_raw_metadata, logger=logger
    )
    # Parse and validate NOMAD dataset metadata with a pydantic model (DatasetMetadata)
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata, logger=logger
    )
    # Save datasets metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetSourceName.NOMAD.value}_{DataType.DATASETS.value}.parquet",
        datasets_normalized_metadata,
        logger=logger,
    )
    # Scrape NOMAD files metadata.
    files_normalized_metadata = scrape_files_for_all_datasets(
        client, datasets_normalized_metadata, logger=logger
    )

    # Save files metadata to parquet file.
    export_list_of_models_to_parquet(
        output_dir_path
        / f"{DatasetSourceName.NOMAD.value}_{DataType.FILES.value}.parquet",
        files_normalized_metadata,
        logger=logger,
    )

    # Print script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    logger.success(f"Scraped NOMAD in: {timedelta(seconds=elapsed_time)} 🎉")


if __name__ == "__main__":
    main()

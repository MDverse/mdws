"""Scrape molecular dynamics simulation datasets and files from the MDDB.

This script extracts molecular dynamics datasets produced within the
MDDB (Molecular Dynamics Data Bank) project, which is distributed across
two nodes:

- MDPOSIT MMB node (https://mmb-dev.mddbr.eu/#/browse)
- MDPOSIT INRIA node https://dynarepo.inria.fr/#/
"""

import json
import sys
from pathlib import Path
from typing import Any

import click
import httpx
import loguru

from ..core.logger import create_logger
from ..core.network import (
    HttpMethod,
    create_httpx_client,
    is_connection_to_server_working,
    make_http_request_with_retries,
)
from ..core.toolbox import print_statistics
from ..models.dataset import DatasetMetadata
from ..models.enums import DatasetSourceName, MoleculeType
from ..models.scraper import ScraperContext
from ..models.simulation import ForceFieldModel, Molecule, Software
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)

MDDB_REPOSITORIES = {
    DatasetSourceName.MDPOSIT_MMB_NODE: "https://mmb-dev.mddbr.eu/api/rest/v1",
    DatasetSourceName.MDPOSIT_INRIA_NODE: "https://inria.mddbr.eu/api/rest/v1",
}


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    node_name: DatasetSourceName,
    page_size: int = 50,
    logger: "loguru.Logger" = loguru.logger,
    scraper: ScraperContext | None = None,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the MDposit API.

    Within the MDposit terminology, datasets are referred to as "projects".

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    query_entry_point: str
        The entry point of the API request.
    node_name: DatasetSourceName
        MDDB node name for logging.
    page_size: int
        Number of entries to fetch per page.
    logger: "loguru.Logger"
        Logger for logging messages.
    scraper: ScraperContext | None
        Optional scraper context. When provided and running in debug mode,
        dataset scraping is intentionally stopped early to limit the amount
        of retrieved data.

    Returns
    -------
    list[dict]:
        A list of MDposit entries.
    """
    logger.info(f"Scraping molecular dynamics datasets from {node_name}.")
    logger.info(f"Using batches of {page_size} datasets.")
    all_datasets = []

    # Start by requesting the first page to get total number of datasets.
    logger.info("Requesting first page to get total number of datasets...")
    page = 0  # start with first page

    while True:
        response = make_http_request_with_retries(
            client,
            f"{query_entry_point}?limit={page_size}&page={page}",
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

            if scraper and scraper.is_in_debug_mode and len(all_datasets) >= 120:
                logger.warning("Debug mode is ON: stopping after 120 datasets.")
                return all_datasets

        except (json.decoder.JSONDecodeError, ValueError) as exc:
            logger.error(f"Error while parsing MDposit response: {exc}")
            logger.error("Jumping to next iteration.")

        page += 1  # increment page for next iteration

    logger.success(f"Scraped {len(all_datasets):,} datasets in MDposit.")
    return all_datasets


def extract_software_and_version(
    dataset_metadata: dict, dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[Software] | None:
    """
    Extract software names and versions from the nested dataset dictionary.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset dictionnary from which to extract molecules information.
    dataset_id: str
        Identifier of the dataset, used for logging.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[Software] | None
        A list of Software instances with `name` and `version` fields, None otherwise.
    """
    try:
        name = dataset_metadata.get("PROGRAM")
        version = dataset_metadata.get("VERSION")
        if not name:
            return None
        return [Software(name=name, version=version)]
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Error parsing software info for dataset {dataset_id}: {e}")
        return None


def extract_forcefields_and_version(
    dataset_metadata: dict, dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[ForceFieldModel] | None:
    """
    Extract forcefield or model names and versions from the nested dataset dictionary.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset dictionnary from which to extract molecules information.
    dataset_id: str
        Identifier of the dataset entry, used for logging.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[ForceFieldModel] | None
        A list of forcefield or model instances with `name` and `version` fields,
        None otherwise.
    """
    try:
        names = dataset_metadata.get("FF")
        # Adding the water model.
        # Exemple: TIP3P.
        water_model = dataset_metadata.get("WAT")
        if water_model:
            names.append(water_model)
        if not names:
            return None
        return [ForceFieldModel(name=name) for name in names]
    except (ValueError, KeyError) as e:
        logger.warning(
            f"Error parsing forcefield or model info for dataset {dataset_id}: {e}"
        )
        return None


def extract_molecules(
    dataset_metadata: dict, dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[Molecule] | None:
    """
    Extract molecule names and types from the nested dataset dictionary.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset dictionnary from which to extract molecules information.
    dataset_id: str
        Identifier of the dataset, used for logging.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[Molecule] | None
        A list of molecules instances with `name` and `type` fields,
        None otherwise.
    """
    molecules = []
    try:
        prot_seqs = dataset_metadata.get("PROTSEQ") or []
        nucl_seqs = dataset_metadata.get("NUCLSEQ") or []
        ligands = dataset_metadata.get("LIGANDS") or []

        for seq in prot_seqs:
            molecules.append(Molecule(name=seq, type=MoleculeType.PROTEIN))

        for seq in nucl_seqs:
            molecules.append(Molecule(name=seq, type=MoleculeType.NUCLEIC))

        for ligand in ligands:
            molecules.append(Molecule(name=ligand))

        if not molecules:
            logger.warning(f"No molecules found in dataset {dataset_id}.")
            return None
        return molecules

    except (ValueError, KeyError) as e:
        logger.warning(f"Error parsing molecules info for dataset {dataset_id}: {e}")
        return None


def extract_datasets_metadata(
    datasets: list[dict[str, Any]],
    node_name: DatasetSourceName,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw MDposit datasets metadata.

    Parameters
    ----------
    datasets: List[Dict[str, Any]]
        List of raw MDposit datasets metadata.
    node_name: DatasetSourceName
        MDDB node name for the dataset url.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset in datasets:
        # Get the dataset id
        dataset_id = dataset.get("accession")
        logger.info(f"Extracting relevant metadata for dataset: {dataset_id}")
        # Create the dataset url depending on the node
        if node_name is DatasetSourceName.MDPOSIT_MMB_NODE:
            dataset_url = f"https://mmb-dev.mddbr.eu/#/id/{dataset_id}/overview"
        elif node_name is DatasetSourceName.MDPOSIT_INRIA_NODE:
            dataset_url = f"https://dynarepo.inria.fr/#/id/{dataset_id}/overview"
        else:
            logger.warning(
                f"Unknown MDDB node '{node_name}'."
                f"Cannot build entry URL for dataset {dataset_id}."
            )

        dataset_metadata = dataset.get("metadata", {})
        links = dataset_metadata.get("CITATION")
        links_list = [links] if links else None
        a = dataset_metadata.get("AUTHORS")
        author_names = a if isinstance(a, list) else [a] if a else None
        metadata = {
            "dataset_repository_name": node_name.value,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset_url,
            "dataset_project_name": DatasetSourceName.MDDB,
            "external_links": links_list,
            "title": dataset_metadata.get("NAME"),
            "date_created": dataset.get("creationDate"),
            "date_last_updated": dataset.get("updateDate"),
            "number_of_files": len(dataset.get("files", [])),
            "author_names": author_names,
            "license": dataset_metadata.get("LICENSE"),
            "description": dataset_metadata.get("DESCRIPTION"),
            "total_number_of_atoms": dataset_metadata.get("atomCount"),
        }
        # Extract simulation metadata if available.
        # Software names with their versions.
        metadata["software"] = extract_software_and_version(
            dataset_metadata, dataset_id, logger
        )
        # Forcefield and model names with their versions.
        metadata["forcefields"] = extract_forcefields_and_version(
            dataset_metadata, dataset_id, logger
        )
        # Molecules with their nb of atoms and number total of atoms.
        metadata["molecules"] = extract_molecules(dataset_metadata, dataset_id, logger)
        # Time step in fs.
        metadata["simulation_timesteps_in_fs"] = [dataset_metadata.get("TIMESTEP")]
        # Temperatures in kelvin
        metadata["simulation_temperatures_in_kelvin"] = [dataset_metadata.get("TEMP")]
        datasets_metadata.append(metadata)
    logger.info(f"Extracted metadata for {len(datasets_metadata)} datasets.")
    return datasets_metadata


def scrape_files_for_one_dataset(
    client: httpx.Client,
    url: str,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """
    Scrape files metadata for a given MDposit dataset.

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    url: str
        The URL endpoint.
    dataset_id: str
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


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    node_base_url: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """Scrape files metadata for all datasets in MDposit API.

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    datasets: list[DatasetMetadata]
        List of datasets to scrape files metadata for.
    node_base_url: str
        Base url of the specific node of MDposit API.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of files metadata dictionaries.
    """
    all_files_metadata = []
    for dataset_count, dataset in enumerate(datasets, start=1):
        dataset_id = dataset.dataset_id_in_repository
        files_metadata = scrape_files_for_one_dataset(
            client,
            url=f"{node_base_url}/projects/{dataset_id}/filenotes",
            dataset_id=dataset_id,
            logger=logger,
        )
        if not files_metadata:
            continue
        # Extract relevant files metadata.
        logger.info(f"Getting files metadata for dataset: {dataset_id}")
        files_metadata = extract_files_metadata(
            files_metadata, node_base_url, dataset, logger=logger
        )
        all_files_metadata += files_metadata
        # Normalize files metadata with pydantic model (FileMetadata)
        logger.info(f"Total files found: {len(all_files_metadata):,}")
        logger.info(
            "Extracted files metadata for "
            f"{dataset_count:,}/{len(datasets):,} "
            f"({dataset_count / len(datasets):.0%}) datasets."
        )
    return all_files_metadata


def extract_files_metadata(
    raw_metadata: dict,
    node_base_url: str,
    dataset: DatasetMetadata,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw MDposit files metadata.

    Parameters
    ----------
    raw_metadata: dict
        Raw files metadata.
    node_base_url: str
        The unique identifier of the dataset in MDposit.
    dataset: DatasetMetadata
        Normalized dataset to scrape files metadata for.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of select files metadata.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    for mdposit_file in raw_metadata:
        dataset_id = dataset.dataset_id_in_repository
        file_name = Path(mdposit_file.get("filename"))
        file_type = file_name.suffix.lstrip(".")
        node_base_url_for_file = node_base_url.replace("/v1", "")
        file_path_url = (
            f"{node_base_url_for_file}/current/projects/{dataset_id}/files/{file_name}"
        )

        parsed_file = {
            "dataset_repository_name": dataset.dataset_repository_name,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset.dataset_url_in_repository,
            "file_name": str(file_name),
            "file_type": file_type,
            "file_size_in_bytes": mdposit_file.get("length", None),
            "file_md5": mdposit_file.get("md5", None),
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
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(output_dir_path: Path, *, is_in_debug_mode: bool = False) -> None:
    """Scrape molecular dynamics datasets and files from MDDB."""
    # Create HTTPX client
    client = create_httpx_client()

    # Iterate over the nodes
    for data_source_name, base_url in MDDB_REPOSITORIES.items():
        # Create scraper context.
        scraper = ScraperContext(
            data_source_name=data_source_name,
            output_dir_path=output_dir_path,
            is_in_debug_mode=is_in_debug_mode,
        )
        # Create logger.
        level = "DEBUG" if scraper.is_in_debug_mode else "INFO"
        logger = create_logger(logpath=scraper.log_file_path, level=level)
        # Print scraper configuration.
        logger.debug(scraper.model_dump_json(indent=4, exclude={"token"}))
        logger.info(f"Starting {data_source_name.name} data scraping...")
        # Check connection to the API
        if is_connection_to_server_working(
            client, f"{base_url}/projects/summary", logger=logger
        ):
            logger.success(f"Connection to {data_source_name} API successful!")
        else:
            logger.critical(f"Connection to {data_source_name} API failed.")
            logger.critical("Aborting.")
            sys.exit(1)

        # Scrape the datasets metadata.
        datasets_raw_metadata = scrape_all_datasets(
            client,
            query_entry_point=f"{base_url}/projects",
            node_name=data_source_name,
            logger=logger,
            scraper=scraper,
        )
        if not datasets_raw_metadata:
            logger.critical(f"No datasets found in {data_source_name}.")
            logger.critical("Aborting.")
            sys.exit(1)

        # Select datasets metadata
        datasets_selected_metadata = extract_datasets_metadata(
            datasets_raw_metadata, data_source_name, logger=logger
        )
        # Validate datasets metadata with the DatasetMetadata Pydantic model.
        datasets_normalized_metadata = normalize_datasets_metadata(
            datasets_selected_metadata, logger=logger
        )
        # Save datasets metadata to parquet file.
        scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
            scraper.datasets_parquet_file_path,
            datasets_normalized_metadata,
            logger=logger,
        )
        # Scrape NOMAD files metadata.
        files_metadata = scrape_files_for_all_datasets(
            client,
            datasets_normalized_metadata,
            base_url,
            logger=logger,
        )
        # Validate NOMAD files metadata with the FileMetadata Pydantic model.
        files_normalized_metadata = normalize_files_metadata(
            files_metadata, logger=logger
        )
        # Save files metadata to parquet file.
        scraper.number_of_files_scraped = export_list_of_models_to_parquet(
            scraper.files_parquet_file_path,
            files_normalized_metadata,
            logger=logger,
        )
        # Print scraping statistics.
        print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()

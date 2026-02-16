"""Scrape metadata of molecular dynamics datasets and files from ATLAS."""

import json
import re
import sys
from pathlib import Path

import click
import httpx
import loguru
from bs4 import BeautifulSoup

from ..core.logger import create_logger
from ..core.network import (
    HttpMethod,
    create_httpx_client,
    is_connection_to_server_working,
    make_http_request_with_retries,
)
from ..core.toolbox import print_statistics
from ..models.dataset import DatasetMetadata
from ..models.enums import DatasetSourceName, ExternalDatabaseName
from ..models.scraper import ScraperContext
from ..models.simulation import ExternalIdentifier, ForceFieldModel, Molecule, Software
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)

INDEX_URL = "https://www.dsimb.inserm.fr/ATLAS/"
BASE_API_URL = "https://www.dsimb.inserm.fr/ATLAS/api"
ATLAS_METADATA = {
    "license": "CC-BY-NC",  # https://www.dsimb.inserm.fr/ATLAS/download.html
    "author_name": [  # https://academic.oup.com/nar/article/52/D1/D384/7438909
        "Yann Vander Meersche",
        "Gabriel Cretin",
        "Aria Gheeraert",
        "Jean-Christophe Gelly",
        "Tatiana Galochkina",
    ],
    "doi": "10.1093/nar/gkad1084",  # https://academic.oup.com/nar/article/52/D1/D384/7438909
    "external_link": ["https://www.dsimb.inserm.fr/ATLAS/"],
    "software_name": "GROMACS",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "software_version": "v2019.6",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "forcefied_name": "CHARMM36m",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "forcefied_version": "July 2020",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "water_model": "TIP3P",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_temperature": 300,  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_time": "100 ns",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_timestep": 2,  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
}


def extract_pdb_chains_from_html(
    html: str, logger: "loguru.Logger" = loguru.logger
) -> set[str]:
    """Extract PDB chain identifiers from ATLAS index page.

    Parameters
    ----------
    html : str
        HTML content of the ATLAS index page.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    set[str]
        Set of PDB chain identifiers found.
    """
    pdb_chains = []
    pdb_chain_pattern = re.compile(
        r"/ATLAS/database/ATLAS/([A-Za-z0-9]{4}_[A-Za-z])/.*html"
    )
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        match = pdb_chain_pattern.search(href)
        if match:
            pdb_chains.append(match.group(1))
    return set(pdb_chains)


def extract_file_sizes_from_html(
    html: str, logger: "loguru.Logger" = loguru.logger
) -> list[dict]:
    """Extract file sizes from ATLAS dataset HTML page.

    Parameters
    ----------
    html : str
        HTML content of the ATLAS dataset page.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of file names, sizes and urls found.

    """
    files_metadata = []
    download_link_pattern = re.compile(
        r"https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/[A-Za-z0-9]{4}_[A-Za-z]/.*zip"
    )
    file_size_pattern = re.compile(r"Download \(([A-Za-z0-9,\. ]+)\)")
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        match_link = download_link_pattern.search(href)
        match_size = file_size_pattern.search(link.text)
        if match_link and match_size:
            files_metadata.append(
                {
                    "file_name": Path(href).name,
                    "file_url_in_repository": href,
                    # File sizes are sometimes expressed with comma
                    # as decimal separator.
                    "file_size_in_bytes": match_size.group(1).replace(",", "."),
                }
            )
    logger.info(f"Found {len(files_metadata)} files in the HTML page.")
    return files_metadata


def scrape_metadata_for_one_dataset(
    client: httpx.Client,
    chain_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """Fetch metadata for a single ATLAS dataset (PDB chain).

    Parameters
    ----------
    client : httpx.Client
        HTTPX client for making requests.
    chain_id : str
        PDB chain identifier.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    dict | None
        Scraped dataset metadata, or None if failed.
    """
    logger.info(f"Scraping metadata for dataset: {chain_id}")
    api_url = f"{BASE_API_URL}/ATLAS/metadata/{chain_id}"
    dataset_url = (
        f"https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{chain_id}/{chain_id}.html"
    )
    response = make_http_request_with_retries(
        client, api_url, HttpMethod.GET, delay_before_request=0.5, logger=logger
    )
    if not response:
        logger.warning(f"Failed to fetch API data for {chain_id}. Skipping.")
        return None
    meta_json = None
    try:
        meta_json = response.json().get(f"{chain_id}")
    except (json.decoder.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to decode JSON response from the ATLAS API.")
        logger.warning(f"Error: {exc}")
        return None
    metadata = {
        "dataset_repository_name": DatasetSourceName.ATLAS,
        "dataset_id_in_repository": chain_id,
        "dataset_url_in_repository": dataset_url,
        "title": meta_json.get("protein_name"),
        "description": meta_json.get("organism"),
        "license": ATLAS_METADATA["license"],
        "author_names": ATLAS_METADATA["author_name"],
        "doi": ATLAS_METADATA["doi"],
        "external_links": ATLAS_METADATA["external_link"],
    }
    # Add molecules.
    external_identifiers = []
    if meta_json.get("PDB"):
        external_identifiers.append(
            ExternalIdentifier(
                database_name=ExternalDatabaseName.PDB,
                identifier=meta_json["PDB"].split("_", maxsplit=1)[0],
            )
        )
    if meta_json.get("UniProt"):
        external_identifiers.append(
            ExternalIdentifier(
                database_name=ExternalDatabaseName.UNIPROT,
                identifier=meta_json["UniProt"],
            )
        )
    metadata["molecules"] = [
        Molecule(
            name=meta_json.get("protein_name"),
            sequence=meta_json.get("sequence"),
            external_identifiers=external_identifiers,
        )
    ]
    # Add software.
    metadata["software"] = [
        Software(
            name=ATLAS_METADATA["software_name"],
            version=ATLAS_METADATA["software_version"],
        )
    ]
    # Add forcefields and models.
    metadata["forcefields_models"] = [
        ForceFieldModel(
            name=ATLAS_METADATA["forcefield_name"],
            version=ATLAS_METADATA["forcefield_version"],
        ),
        ForceFieldModel(name=ATLAS_METADATA["water_model"]),
    ]
    # Add simulation temperature.
    metadata["simulation_temperatures_in_kelvin"] = [
        ATLAS_METADATA["simulation_temperature"]
    ]
    # Add simulation time.
    metadata["simulation_times"] = [ATLAS_METADATA["simulation_time"]]
    # Add simulation time step.
    metadata["simulation_timesteps_in_fs"] = [ATLAS_METADATA["simulation_timestep"]]
    logger.info("Done.")
    return metadata


def search_all_datasets(client: httpx.Client, logger: "loguru.Logger") -> set[str]:
    """Search for ATLAS datasets (1 dataset = 1 PDB chain).

    Parameters
    ----------
    client : httpx.Client
        HTTPX client for making requests.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    set[str]
        Set of PDB chains (datasets) found.
    """
    logger.info("Fetching index page listing ATLAS datasets...")
    response = make_http_request_with_retries(
        client, INDEX_URL, HttpMethod.GET, delay_before_request=0.5, logger=logger
    )
    if not response:
        logger.critical("Failed to fetch index page.")
        logger.critical("Cannot list available datasets. Aborting!")
        sys.exit(1)
    if not hasattr(response, "text") or not response.text:
        logger.critical("Index page response is empty.")
        logger.critical("Cannot list available datasets. Aborting!")
        sys.exit(1)
    chain_ids = extract_pdb_chains_from_html(response.text, logger=logger)
    logger.info(f"Found {len(chain_ids)} datasets.")
    return chain_ids


def scrape_all_datasets(
    client: httpx.Client,
    pdb_chains: set[str],
    logger: "loguru.Logger",
) -> list[dict]:
    """Scrape all ATLAS datasets given a set of PDB chains.

    Parameters
    ----------
    pdb_chains : set[str]
        Set of PDB chains to scrape.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of scraped dataset metadata.
    """
    datasets_meta = []
    logger.info("Starting scraping of all datasets...")
    for pdb_counter, pdb_chain in enumerate(pdb_chains, start=1):
        metadata = scrape_metadata_for_one_dataset(client, pdb_chain, logger=logger)
        if metadata:
            datasets_meta.append(metadata)
        logger.info(
            f"Scraped {pdb_counter:,}/{len(pdb_chains):,} "
            f"({pdb_counter / len(pdb_chains):.0%}) datasets"
        )
    return datasets_meta


def scrape_all_files(
    client: httpx.Client,
    datasets_metadata: list[DatasetMetadata],
    logger: "loguru.Logger",
) -> list[dict]:
    """Scrape ATLAS files.

    Parameters
    ----------
    datasets_metadata : list[DatasetMetadata]
        List of datasets metadata.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of scraped files metadata.
    """
    files_metadata = []
    for dataset_counter, dataset_meta in enumerate(datasets_metadata, start=1):
        pdb_chain = dataset_meta.dataset_id_in_repository
        logger.info(f"Scraping files metadata for dataset: {pdb_chain}")
        url = dataset_meta.dataset_url_in_repository
        response = make_http_request_with_retries(
            client, url, HttpMethod.GET, delay_before_request=0.5, logger=logger
        )
        if not response:
            logger.warning(f"Failed to fetch HTML page for {pdb_chain}. Skipping.")
            continue
        files_meta = extract_file_sizes_from_html(response.text, logger=logger)
        for meta in files_meta:
            metadata = {
                "dataset_repository_name": dataset_meta.dataset_repository_name,
                "dataset_id_in_repository": dataset_meta.dataset_id_in_repository,
                "dataset_url_in_repository": dataset_meta.dataset_url_in_repository,
                "file_name": meta["file_name"],
                "file_url_in_repository": meta["file_url_in_repository"],
                "file_size_in_bytes": meta["file_size_in_bytes"],
            }
            files_metadata.append(metadata)
        logger.info(
            "Scraped metadata files for "
            f"{dataset_counter:,}/{len(datasets_metadata):,} "
            f"({dataset_counter / len(datasets_metadata):.0%}) datasets"
        )
        logger.info(f"Total files scraped so far: {len(files_metadata):,}")
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
    """Scrape metadata of molecular dynamics datasets and files from ATLAS."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.ATLAS,
        output_dir_path=output_dir_path,
        is_in_debug_mode=is_in_debug_mode,
    )
    # Create logger.
    level = "INFO"
    if scraper.is_in_debug_mode:
        level = "DEBUG"
    logger = create_logger(logpath=scraper.log_file_path, level=level)
    # Print scraper configuration.
    logger.debug(scraper.model_dump_json(indent=4, exclude={"token"}))
    logger.info("Starting ATLAS data scraping...")
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to the ATLAS API
    if is_connection_to_server_working(
        client, f"{BASE_API_URL}/ATLAS/metadata/16pk_A", logger=logger
    ):
        logger.success("Connection to ATLAS API successful!")
    else:
        logger.critical("Connection to ATLAS API failed.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Scrape datasets metadata.
    datasets_ids = search_all_datasets(client=client, logger=logger)
    if scraper.is_in_debug_mode:
        datasets_ids = set(list(datasets_ids)[:10])
        logger.warning("Debug mode is ON: limiting to first 10 datasets.")
    datasets_metadata = scrape_all_datasets(
        client,
        datasets_ids,
        logger=logger,
    )
    # Normalize datasets metadata.
    datasets_metadata_normalized = normalize_datasets_metadata(
        datasets_metadata,
        logger=logger,
    )
    # Scrape files metadata.
    files_metadata = scrape_all_files(
        client,
        datasets_metadata_normalized,
        logger=logger,
    )
    # Normalize datasets metadata.
    files_metadata_normalized = normalize_files_metadata(
        files_metadata,
        logger=logger,
    )
    # Save datasets metadata to parquet file.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_metadata_normalized,
        logger=logger,
    )
    # Save files metadata to parquet file.
    scraper.number_of_files_scraped = export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_metadata_normalized,
        logger=logger,
    )
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()

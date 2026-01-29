"""Scrape molecular dynamics simulation datasets and files from GPCRmd.

This script scrapes molecular dynamics datasets from the GPCRmd repository
https://www.gpcrmd.org/dynadb/search/
"""

import json
import sys
from pathlib import Path
from typing import Any

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
    retrieve_file_size_from_http_head_request,
)
from ..core.toolbox import print_statistics
from ..models.enums import DatasetSourceName
from ..models.scraper import ScraperContext
from ..models.simulation import ForceFieldModel, Molecule, Software
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)

BASE_GPCRMD_URL = "https://www.gpcrmd.org/api/search_all"


def scrape_all_datasets(
    client: httpx.Client,
    url: str,
    scraper: ScraperContext,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the GPCRmd API.

    Within the NOMAD terminology, datasets are referred to as "dynamic".
    Doc: https://gpcrmd.org/api/

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL of the API request.
    logger: "loguru.Logger"
        Logger for logging messages.
    scraper: ScraperContext
        Pydantic model describing the context of a scraper

    Returns
    -------
    list[dict]:
        A list of GPCRmd entries.
    """
    logger.info("Scraping molecular dynamics datasets from GPCRmd.")
    logger.info("Requesting all datasets in a single fetch...")
    all_datasets = []
    response = make_http_request_with_retries(
        client,
        url,
        method=HttpMethod.GET,
        timeout=60,
        delay_before_request=0.2,
    )
    if not response:
        logger.critical("Failed to fetch data from GPCRmd API.")
        sys.exit(1)
    else:
        try:
            # Get the formatted response with request metadatas in JSON format
            all_datasets = response.json()
        except (json.decoder.JSONDecodeError, ValueError) as exc:
            logger.error(f"Error while parsing GPCRmd response: {exc}")
            logger.error("Cannot find datasets.")
            logger.critical("Aborting.")
            sys.exit(1)

    if scraper and scraper.is_in_debug_mode and len(all_datasets) >= 10:
        logger.warning("Debug mode is ON: stopping after 10 datasets.")
        # Return only the first 10 datasets for testing purposes.
        return all_datasets[:10]

    logger.success(f"Scraped {len(all_datasets)} datasets in GPCRmd.")
    return all_datasets


def fetch_all_datasets_html_pages(
    client: httpx.Client, datasets: list[dict], logger: "loguru.Logger" = loguru.logger
) -> list[str | None]:
    """Fetch all datasets HTML pages.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : List[Dict[str, Any]]
        List of raw GPCRmd datasets metadata.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[str | None]
        The HTML content of datasets pages.
    """
    logger.info("Fetching HTML content for all datasets from the GPCRmd repository")
    datasets_html_pages = []

    for dataset_counter, dataset in enumerate(datasets, start=1):
        # Get the URL of the current dataset
        dataset_id = str(dataset.get("dyn_id"))
        logger.info(f"Scraping dataset {dataset_id}:")
        url = dataset.get("url")
        html_content = None
        # If the dataset has a URL, attempt to fetch its HTML content
        if url:
            response = make_http_request_with_retries(
                client,
                url,
                method=HttpMethod.GET,
                timeout=60,
                delay_before_request=0.2,
            )
            # If the request was successful
            if response:
                # Store the HTML text
                html_content = response.text
                logger.success(f"Dataset {dataset_id} fetched successfully")
                logger.debug(f"(HTML length: {len(html_content)} characters)")
            else:
                logger.warning(f"Failed to fetch HTML page for dataset {dataset_id}")
        datasets_html_pages.append(html_content)
        logger.info(
            f"Scraped {dataset_counter:,}/{len(datasets):,} "
            f"({dataset_counter / len(datasets):.0%}) datasets"
        )
    return datasets_html_pages


def _extract_molecules_from_lines(lines: list[str]) -> list[Molecule] | None:
    """
    Extract a list of molecules from text lines.

    The function looks for a "Number of molecules" section and parses
    subsequent lines expected to follow the format "Name: count".
    Parsing stops as soon as the format no longer matches this pattern.

    Parameters
    ----------
    lines : list[str]
        Text lines extracted from the HTML content.

    Returns
    -------
    list[Molecule] | None
        A list of Molecule objects if at least one molecule is found,
        otherwise None.
    """
    molecules: list[Molecule] = []
    capture = False

    for line in lines:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Start capturing after the "Number of molecules" header
        if "Number of molecules" in line:
            capture = True
            continue

        if capture:
            # Stop when the expected "Name: number" format is no longer met
            if ":" not in line or "Total" in line:
                break

            name, count = line.split(":", 1)

            # Stop if the count is not a valid integer
            if not count.strip().isdigit():
                break

            molecules.append(
                Molecule(
                    name=name.strip(),
                    number_of_atoms=int(count.strip()),
                )
            )

    return molecules or None


def retrieve_metadata_from_html_dataset_page(
    html: str,
    field_name: str | None,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[str] | list[Molecule] | None:
    """
    Retrieve a specific metadata field from a webpage.

    Parameters
    ----------
    html : str
        The HTML content of the page.
    field_name : str
        The name of the metadata field to extract (case-sensitive).
        Must appear in the HTML content in the exact format "Field_name:".
    dataset_id: str
        The unique identifier of the dataset in GPCRmd.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[str] | list[Molecule] | None
        The value of the metadata field in a list if found, otherwise None.

    """
    try:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        # Extract the text content from the parsed HTML
        soup_text = soup.text
        # Re-parse the extracted text as HTML
        soup = BeautifulSoup(soup_text, "html.parser")
        # Split the full text into separate lines for line-by-line processing
        lines = soup.get_text(separator="\n").splitlines()

        # Special case for molecules and their number of atoms
        if field_name == "Number of molecules":
            return _extract_molecules_from_lines(lines)

        for line in lines:
            if field_name not in line:
                continue

            # Special case for DOI
            if field_name == "doi" and "doi:" in line:
                doi = line.split("doi:", 1)[1].strip()
                # Usually the doi is at the end of the sentence
                if doi and doi.endswith("."):
                    # So we remove the period
                    doi = doi[:-1]
                return [f"https://doi.org/{doi}"]
            else:
                separator = f"{field_name}:"
                if separator in line:
                    # Return the text after the separator, stripped
                    return [line.split(separator, 1)[1].strip()]

    except (AttributeError, TypeError, ValueError) as exc:
        logger.warning(
            f"Error parsing field '{field_name}' for dataset {dataset_id}: {exc}"
        )

    return None


def scrape_files_metadata_for_one_dataset(
    client: httpx.Client,
    html_content: str | None,
    core_metadata: dict[str, Any],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape files metadata for a given dataset.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    html_content: str | None
        Html content of the dataset web page.
    core_metadata : dict[str, Any]
        Dictionary of dataset core metadata.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of files metadata dictionaries.
    """
    logger.info(
        "Scraping files metadata for dataset: "
        f"{core_metadata['dataset_id_in_repository']}"
    )
    files_metadata = []
    # Extract metadata from dataset url page if available.
    if not html_content:
        logger.error("Failed to fetch files metadata.")
        return files_metadata

    # Find all <a> tags with href containing the files path.
    # Example of files found for dataset 2316:
    # Dataset URL: https://www.gpcrmd.org/dynadb/dynamics/id/2316/
    # /dynadb/files/Dynamics/dyn2667/tmp_dyn_0_2667.pdb
    # /dynadb/files/Dynamics/dyn2667/25399_dyn_2316.psf
    # /dynadb/files/Dynamics/dyn2667/25400_trj_2316.dcd
    # /dynadb/files/Dynamics/dyn2667/25401_trj_2316.dcd
    # /dynadb/files/Dynamics/dyn2667/25402_trj_2316.dcd
    # /dynadb/files/Dynamics/dyn2667/25403_prm_2316.prmtop
    # /dynadb/files/Dynamics/dyn2667/25404_prt_2316.tgz
    soup = BeautifulSoup(html_content, "html.parser")
    for link in soup.find_all("a", href=True):
        href_value = link.get("href", "").strip()
        if "/dynadb/files/Dynamics/" not in href_value:
            continue
        # Add core metadata.
        metadata = core_metadata.copy()
        # Add URL and file name.
        metadata["file_url_in_repository"] = f"https://www.gpcrmd.org/{href_value}"
        metadata["file_name"] = Path(metadata["file_url_in_repository"]).name
        # Fetch the file size using a HEAD request.
        # We do not download the entire file.
        metadata["file_size_in_bytes"] = retrieve_file_size_from_http_head_request(
            client, metadata["file_url_in_repository"], logger=logger
        )
        files_metadata.append(metadata)

    logger.info(f"Total files found: {len(files_metadata):,}")
    return files_metadata


def extract_datasets_and_files_metadata(
    client: httpx.Client,
    datasets: list[dict[str, Any]],
    datasets_html_page: list[str | None],
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[list[dict], list[dict]]:
    """
    Extract relevant metadata from raw GPCRmd datasets metadata.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : list[dict[str, Any]]
        List of raw GPCRmd datasets metadata.
    datasets_html_page: list[str | None]
        List of html content of the dataset web page.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    list[dict]
        List of file metadata dictionaries.
    """
    datasets_metadata = []
    files_metadata = []

    for dataset, html_content in zip(datasets, datasets_html_page, strict=True):
        dataset_id = str(dataset.get("dyn_id"))
        logger.info(f"Extracting metadata for dataset: {dataset_id}")
        dataset_source_name = DatasetSourceName.GPCRMD
        dataset_url = dataset.get("url")
        core_metadata = {
            "dataset_repository_name": dataset_source_name,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset_url,
        }
        metadata = {
            **core_metadata,
            "title": dataset.get("modelname"),
            "date_created": dataset.get("creation_timestamp"),
            "total_number_of_atoms": dataset.get("atom_num"),
        }
        # Convert the timestep string (e.g., "4.0 fs")
        # to a float representing the number of femtoseconds
        timestep = dataset.get("timestep")
        if not isinstance(timestep, float) and timestep is not None:
            timestep = dataset.get("timestep").split()[0]
        metadata["simulation_timesteps_in_fs"] = [timestep]
        # Extract simulation metadata from the API if available.
        # Software names with their versions.
        software = None
        if dataset.get("mysoftware"):
            software = [
                Software(
                    name=dataset["mysoftware"],
                    version=dataset.get("software_version"),
                )
            ]
        metadata["software"] = software
        # Forcefields and models names with their versions.
        forcefields_and_models = None
        if dataset.get("forcefield"):
            forcefields_and_models = [
                ForceFieldModel(
                    name=dataset["forcefield"],
                    version=dataset.get("forcefield_version"),
                )
            ]
        metadata["forcefields"] = forcefields_and_models
        # Molecule names with their number of atoms.
        metadata["molecules"] = retrieve_metadata_from_html_dataset_page(
            html=html_content, field_name="Number of molecules", dataset_id=dataset_id
        )
        # Extract other metadata from dataset url page if available.
        if html_content is None:
            logger.warning(
                "Error parsing additionnal metadatas from web page for dataset"
                f" {dataset_id} ({dataset_url})"
            )
            logger.warning("Skipping this step.")
            datasets_metadata.append(metadata)
            continue

        # Author names.
        metadata["author_names"] = retrieve_metadata_from_html_dataset_page(
            html=html_content, field_name="Submitted by", dataset_id=dataset_id
        )
        # Description.
        description = retrieve_metadata_from_html_dataset_page(
            html=html_content, field_name="Description", dataset_id=dataset_id
        )
        metadata["description"] = description[0] if description else None
        # Simulation time.
        metadata["simulation_times"] = retrieve_metadata_from_html_dataset_page(
            html=html_content,
            field_name="Accumulated simulation time",
            dataset_id=dataset_id,
        )
        # Reference links.
        metadata["external_links"] = retrieve_metadata_from_html_dataset_page(
            html=html_content, field_name="doi", dataset_id=dataset_id
        )
        # Retrieve the files metadata from the html content of the dataset page.
        files_metadata_for_this_dataset = scrape_files_metadata_for_one_dataset(
            client, html_content, core_metadata, logger=logger
        )
        files_metadata.extend(files_metadata_for_this_dataset)
        # Number of files.
        if files_metadata_for_this_dataset:
            metadata["number_of_files"] = len(files_metadata_for_this_dataset)
        # Adding full metadata for the dataset.
        datasets_metadata.append(metadata)
        logger.info(
            f"Scraped metadata for {len(datasets_metadata):,}/{len(datasets):,} "
            f"({len(datasets_metadata) / len(datasets):.0%}) datasets "
            f"({len(files_metadata):,} files)"
        )

    logger.info(f"Extracted metadata for {len(datasets_metadata)} datasets.")
    logger.info(f"Extracted metadata for {len(files_metadata)} files.")
    return datasets_metadata, files_metadata


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
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(output_dir_path: Path, *, is_in_debug_mode: bool = False) -> None:
    """Scrape molecular dynamics datasets and files from GPCRmd."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.GPCRMD,
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
    logger.info("Starting GPCRmd scraping...")
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to GPCRmd API
    if is_connection_to_server_working(
        client, f"{BASE_GPCRMD_URL}/pdbs/", logger=logger
    ):
        logger.success("Connection to GPCRmd API successful!")
    else:
        logger.critical("Connection to GPCRmd API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape GPCRmd datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client=client,
        url=f"{BASE_GPCRMD_URL}/info/",
        scraper=scraper,
        logger=logger,
    )
    if not datasets_raw_metadata:
        logger.critical("No datasets found in GPCRmd.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Send the first dataset raw metadata to the debug log.
    logger.debug("First dataset raw metadata:")
    logger.debug(datasets_raw_metadata[0])
    # Fetch the dataset HTML page for all datasets
    datasets_html_page = fetch_all_datasets_html_pages(
        client, datasets_raw_metadata, logger=logger
    )
    # Extract datasets and files metadata
    datasets_selected_metadata, files_metadata = extract_datasets_and_files_metadata(
        client, datasets_raw_metadata, datasets_html_page, logger=logger
    )
    # Validate GPCRmd datasets metadata with the DatasetMetadata Pydantic model.
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata, logger=logger
    )
    # Save datasets metadata to parquet file.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_normalized_metadata,
        logger=logger,
    )
    # Validate GPCRmd files metadata with the FileMetadata Pydantic model.
    files_normalized_metadata = normalize_files_metadata(files_metadata, logger=logger)
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

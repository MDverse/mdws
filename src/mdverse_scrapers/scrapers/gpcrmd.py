"""Scrape molecular dynamics simulation datasets and files from GPCRmd."""


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

BASE_GPCRMD_URL = "https://www.gpcrmd.org/api/search_all/"


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
    else:
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


def fetch_all_datasets_page(
    client: httpx.Client,
    datasets: list[dict],
    logger: "loguru.Logger" = loguru.logger
) -> list[str | None]:
    """Fetch an dataset page and return its HTML content.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : List[Dict[str, Any]]
        List of raw GPCRmd datasets metadata.

    Returns
    -------
    str | None
        The HTML content of the page if the request is successful, otherwise None.
    """
    datasets_html_page = []
    for dataset in datasets:
        url = dataset.get("url")
        html_content = None
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
        datasets_html_page.append(html_content)

    return datasets_html_page


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


def retrieve_reference_links(html: str) -> list[str]:
    """
    Retrieve reference URLs from the References section of a GPCRMD dataset page.

    Parameters
    ----------
    html : str
        The HTML content of the page.

    Returns
    -------
    list[str] | None
        List of reference URLs (starting with http:// or https://) if found,
        otherwise None.
    """
    soup = BeautifulSoup(html, "html.parser")

    header = next(
        (
            h
            for h in soup.find_all("h3")
            if h.get_text(strip=True) == "References"
        ),
        None,
    )
    if not isinstance(header, Tag):
        return []

    content_div = header.find_next_sibling("div", class_="techinfo_content")
    if not isinstance(content_div, Tag):
        return []

    return [
        a["href"].strip()
        for a in content_div.find_all("a", href=True)
        if isinstance(a, Tag)
        and a["href"].strip().startswith(("http://", "https://"))
    ]


def count_simulation_files(html: str) -> int | None:
    """
    Count files in the dataset webpage.

    Especially in 'Simulation output files' and 'Simulation protocol \
    & starting files' sections.

    Parameters
    ----------
    html : str
        The HTML content of the page.

    Returns
    -------
    int | None
        The number of files related to this dataset.
    """
    if html:
        # Parse the HTML content
        soup = BeautifulSoup(html, "html.parser")

        # Helper function to count unique links in a container div
        def count_links(container_id: str) -> int:
            # Find the container <div> by ID
            container = soup.find("div", id=container_id)
            # Ensure the container is actually a Tag
            if not isinstance(container, Tag):
                return 0

            # Collect all hrefs in <a> tags, stripping whitespace
            links = [
                str(a.get("href", "")).strip()
                for a in container.find_all("a", href=True)
                if isinstance(a, Tag) and str(a.get("href", "")).strip()
            ]

            # Remove duplicates while preserving order
            return len(dict.fromkeys(links))

        output_files_count = count_links("allfiles")
        protocol_files_count = count_links("paramfiles")
        return output_files_count + protocol_files_count
    return None


def extract_datasets_metadata(
    client: httpx.Client,
    datasets: list[dict[str, Any]],
    datasets_html_page: list[str | None],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw GPCRmd datasets metadata.

    Parameters
    ----------
    datasets : list[dict[str, Any]]
        List of raw GPCRmd datasets metadata.
    datasets_html_page: list[str | None]
        List of html content of the dataset web page.
    client : httpx.Client
        The HTTPX client to use for making requests.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset, html_content in zip(datasets, datasets_html_page, strict=True):
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
            "software_name": dataset.get("mysoftware"),
            "software_version": dataset.get("software_version"),
            "forcefield_model_name": dataset.get("forcefield"),
            "forcefield_model_version": dataset.get("forcefield_version"),
            "simulation_timestep": dataset.get("timestep"),
            "nb_atoms": dataset.get("atom_num")
        }
        # Extract other metadata from dataset url page if available.
        if html_content is None:
            logger.warning(
                "Error parsing additionnal metadatas from web page for dataset"
                f" {dataset_id} ({dataset_url})")
            logger.warning("Skipping this step.")
            datasets_metadata.append(metadata)
            continue

        # Author names.
        author_names = None
        try:
            author_names = retrieve_metadata(html_content, "Submitted by")
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing author names for entry {dataset_id}: {e}")
        metadata["author_names"] = (author_names if author_names
                                    is None else [author_names])
        # Description.
        description = None
        try:
            description = retrieve_metadata(html_content, "Description")
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing description for entry {dataset_id}: {e}")
        metadata["description"] = description
        # Simulation time.
        stime_list = None
        try:
            stime = retrieve_metadata(html_content, "Accumulated simulation time")
            stime_list = [stime] if stime else []
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing simulation time for entry {dataset_id}: {e}")
        metadata["simulation_time"] = stime_list
        # Reference links.
        refs = None
        try:
            refs = retrieve_reference_links(html_content)
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing reference links for entry {dataset_id}: {e}")
        metadata["external_links"] = refs
        # Number of files.
        nb_files = None
        try:
            nb_files: int | None = count_simulation_files(html_content)
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing number of files for entry {dataset_id}: {e}")
        metadata["nb_files"] = nb_files
        # Molecule names.
        molecule_names = None
        try:
            dyncomp: list[dict[str, Any]] = dataset.get("dyncomp", [])
            molecule_names: list[str] = (
                [comp.get("resname") for comp in dyncomp if comp.get("resname")])
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing molecule names for entry {dataset_id}: {e}")
        metadata["molecule_names"] = molecule_names
        # Adding full metadatas of the dataset
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


def extract_files_metadata_from_html(
    client: httpx.Client,
    html_content: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[tuple[str, str, int, str]]:
    """
    Extract relevant metadata from raw GPCRmd files metadata.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    html_content : str
        HTML content of the dataset page.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[tuple[str, str, int, str]]
        Tuples of (file_name, file_type, int, file_url).
        Empty if none found.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all <a> tags with href containing the files path
    for link in soup.find_all("a", href=True):
        href_value = link.get("href", "").strip()
        if not href_value or "/dynadb/files/Dynamics/" not in href_value:
            continue

        file_url = f"https://www.gpcrmd.org/{href_value}"
        # Example of file urls:
        # From dataset ID:  2316 (https://www.gpcrmd.org/dynadb/dynamics/id/2316/)
        # 1. https://www.gpcrmd.org/dynadb/files/Dynamics/dyn2667/tmp_dyn_0_2667.pdb
        # 2. https://www.gpcrmd.org/dynadb/files/Dynamics/dyn2667/25400_trj_2316.dcd

        file_name = Path(file_url).name
        file_type = Path(file_name).suffix.lstrip(".").lower()

        # Fetch the file size using a HEAD request
        response = make_http_request_with_retries(
            client,
            file_url,
            method=HttpMethod.HEAD,
            timeout=60,
            delay_before_request=0.2,
        )
        if response and response.headers:
            file_size = int(response.headers.get("Content-Length", 0))
        else:
            file_size = None
            logger.warning(f"Could not retrieve file size for '{file_name}'")

        files_metadata.append((file_name, file_type, file_size, file_url))

    return files_metadata


def scrape_files_for_one_dataset(
    client: httpx.Client,
    url: str,
    html_content: str | None,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict] | None:
    """
    Scrape files metadata for a given GPCRmd dataset.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    html_content: str | None
        Html content of the dataset web page.
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
    files_metadata: list[dict] = []
    datasets_metadata = {
        "dataset_repository_name": DatasetRepositoryName.GPCRMD,
        "dataset_id_in_repository": dataset_id,
        "dataset_url_in_repository": url,
    }

    # Extract metadata from dataset url page if available.
    if not html_content:
        logger.error("Failed to fetch files metadata.")
        return None

    for file_name, file_type, file_size, file_url in extract_files_metadata_from_html(
        client, html_content, logger
    ):
        file_metadata = {
            **datasets_metadata,
            "file_name": file_name,
            "file_type": file_type,
            "file_size_in_bytes": file_size,
            "file_url_in_repository": file_url,
            "date_last_fetched": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        files_metadata.append(file_metadata)

    return files_metadata


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    datasets_html_page: list[str | None],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """Scrape files metadata for all datasets in GPCRmd.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    datasets : list[DatasetMetadata]
        List of datasets to scrape files metadata for.
    datasets_html_page: list[str | None]
        List of html content of the dataset web page.
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
        dataset_html_page = datasets_html_page[dataset_count - 1]
        files_metadata = scrape_files_for_one_dataset(
            client,
            url=dataset.dataset_url_in_repository,
            html_content=dataset_html_page,
            dataset_id=dataset_id,
            logger=logger,
        )
        if not files_metadata:
            continue
        # Normalize files metadata with pydantic model (FileMetadata)
        logger.info(f"Validating files metadata for dataset: {dataset_id}")
        for file_metadata in files_metadata:
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
    output_dir_path = (output_dir_path / DatasetProjectName.GPCRMD.value
                       / datetime.now().strftime("%Y-%m-%d"))
    output_dir_path.mkdir(parents=True, exist_ok=True)
    logfile_path = output_dir_path / f"{DatasetProjectName.GPCRMD.value}_scraper.log"
    logger = create_logger(logpath=logfile_path, level="INFO")
    logger.info("Starting GPCRmd data scraping...")
    start_time = time.perf_counter()
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to GPCRmd API
    if is_gpcrmd_connection_working(client, f"{BASE_GPCRMD_URL}pdbs/"):
        logger.success("Connection to GPCRmd API successful!")
    else:
        logger.critical("Connection to GPCRmd API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape GPCRmd datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client,
        query_entry_point=f"{BASE_GPCRMD_URL}info/",
        logger=logger,
    )
    if not datasets_raw_metadata:
        logger.critical("No datasets found in GPCRmd.")
        logger.critical("Aborting.")
        sys.exit(1)
    datasets_raw_metadata = datasets_raw_metadata[:4]

    # Fetch the dataset page for all datasets
    datasets_html_page = fetch_all_datasets_page(
        client, datasets_raw_metadata, logger
    )
    # Select datasets metadata
    datasets_selected_metadata = extract_datasets_metadata(
        client, datasets_raw_metadata, datasets_html_page, logger=logger
    )
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
        client, datasets_normalized_metadata, datasets_html_page, logger=logger
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


if __name__ == "__main__":
    main()

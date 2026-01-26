"""Scrape molecular dynamics datasets and files from Zenodo."""

import json
import os
import sys
from pathlib import Path

import click
import loguru
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from ..core.logger import create_logger
from ..core.toolbox import (
    clean_text,
    find_remove_false_positive_datasets,
    make_http_get_request_with_retries,
    print_statistics,
    read_query_file,
    remove_duplicates_in_list_of_dicts,
    remove_excluded_files,
)
from ..models.enums import DatasetSourceName
from ..models.file import FileMetadata
from ..models.scraper import ScraperContext
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)


def get_rate_limit_info(
    url_lst: list[str], token: str, logger: "loguru.Logger" = loguru.logger
) -> None:
    """Get rate limit information from Zenodo API endpoints.

    Parameters
    ----------
    url_lst : list of str
        List of URLs to send HEAD requests to.
    token : str
        Zenodo API token.
    logger : loguru.Logger, optional
        Logger for logging messages.
    """
    for url in url_lst:
        response = make_http_get_request_with_retries(
            url=url,
            params={"token": token},
            timeout=60,  # Zenodo serveur can be sometimes slow to respond.
            max_attempts=1,
            delay_before_request=2,
            logger=logger,
        )
        if response is None:
            logger.error(f"Cannot connect to: {url}")
            continue
        logger.info(f"Rate limit info from {url}:")
        logger.info(
            "Header x-ratelimit-limit: "
            f"{response.headers.get('X-ratelimit-limit', None)}"
        )
        logger.info(
            "Header x-ratelimit-remaining: "
            f"{response.headers.get('X-ratelimit-remaining', None)}"
        )
        logger.info(
            "Header x-ratelimit-reset: "
            f"{response.headers.get('X-ratelimit-reset', None)}"
        )
        logger.info(f"Header retry-after: {response.headers.get('retry-after', None)}")


def get_files_structure_from_zip(ul):
    """Get files structure from zip file preview.

    Recursion based on:
    https://stackoverflow.com/questions/17850121/parsing-nested-html-list-with-beautifulsoup

    Example:
    https://zenodo.org/records/7756756/preview/Glycerol020.zip

    <ul class="tree list-unstyled">
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item0">Glycerol020 </a>
        </div>
      </div><ul id="tree_item0">
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item3">Em2 </a>
        </div>
      </div><ul id="tree_item3">
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item15">Flow </a>
        </div>
      </div><ul id="tree_item15">
      <li>
        <div class="ui equal width grid">
        <div class="row">
          <div class="no-padding left floated column">
            <span><i class="file outline icon"></i></i> flow_00001.dat</span>
          </div>
          <div class="no-padding right aligned column">4.6 kB</div>
        </div>
      </div>
      </li>
      <li>
        <div class="ui equal width grid">
        <div class="row">
          <div class="no-padding left floated column">
            <span><i class="file outline icon"></i></i> flow_00003.dat</span>
          </div>
          <div class="no-padding right aligned column">4.6 kB</div>
        </div>
      </div>
      </li>

    Parameters
    ----------
    ul : bs4.element
        HTML table containing the files structure.

    Returns
    -------
    dict
        Nested dictionary with files structure.
    """
    structure = {}
    for li in ul.find_all("li", recursive=False):
        # Extract folrder or file name.
        key = next(li.stripped_strings)
        ul = li.find("ul")
        if ul:
            structure[key] = get_files_structure_from_zip(ul)
        # Extract file size.
        elif li.find("div", attrs={"class": "no-padding right aligned column"}):
            structure[key] = li.find(
                "div", attrs={"class": "no-padding right aligned column"}
            ).text
        else:
            structure[key] = None
    return structure


def extract_data_from_zip_file(url, logger: "loguru.Logger" = loguru.logger):
    """Extract data from zip file preview.

    Examples of zip file previews:
    - https://zenodo.org/records/4444751/preview/code.zip
    - https://zenodo.org/records/16412906/preview/DPPS200_HN45_0.25M-NaCl_TIP3P_353.15K_prod.zip

    url : str
        URL of zip file preview

    Returns
    -------
    list
        List of dictionnaries with data extracted from zip preview.
    """
    file_lst = []
    response = make_http_get_request_with_retries(
        url, delay_before_request=2, timeout=30, max_attempts=5
    )
    if response is None:
        return file_lst
    if "Zipfile is not previewable" in response.text:
        logger.warning(f"No preview available for: {url}")
        logger.warning("Skipping zip file.")
        return file_lst
    # Scrap HTML content.
    soup = BeautifulSoup(response.content, "html5lib")
    table = soup.find("ul", attrs={"class": "tree list-unstyled"})
    files_structure = get_files_structure_from_zip(table)
    # Convert nested dictionary files structure to a flat dictionary.
    df = pd.json_normalize(files_structure, sep="/")
    # Handle case with zip file with no files.
    # For instance:
    # https://zenodo.org/records/15878278/preview/data_naresh.zip
    try:
        files_dict = df.to_dict(orient="records")[0]
    except IndexError:
        return file_lst
    # Normalize file size.
    for path, size in files_dict.items():
        if size:
            file_lst.append(
                {
                    "file_name": path,
                    "file_size_in_bytes": size,
                }
            )
    logger.success(f"Found {len(file_lst)} files.")
    return file_lst


def is_zenodo_connection_working(
    token: str, logger: "loguru.Logger" = loguru.logger
) -> bool:
    """Test connection to Zenodo API.

    Zenodo HTTP status codes are listed here:
    https://developers.zenodo.org/#http-status-codes

    Parameters
    ----------
    token : str
        Token for Zenodo API

    Returns
    -------
    bool
        True if connection is successful, False otherwise.
    """
    logger.info("Trying connection to Zenodo...")
    response = make_http_get_request_with_retries(
        url="https://zenodo.org/api/deposit/depositions",
        params={"access_token": token},
        timeout=10,
        max_attempts=2,
        logger=logger,
    )
    if not response:
        logger.error("Cannot connect to the Zenodo API.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def scrap_zip_content(
    files_metadata: list[FileMetadata], logger: "loguru.Logger" = loguru.logger
) -> list[dict]:
    """Scrap information from files contained in zip archives.

    Zenodo provides a preview only for the first 1000 files within a zip file.
    See:
    https://github.com/inveniosoftware/invenio-previewer/blob/
    8ecdd4299b80a83d39679859fcedae47d68b012c/invenio_previewer/
    extensions/zip.py#L28

    Arguments
    ---------
    files_metadata: list[FileMetadata]
        List of files metadata.

    Returns
    -------
    list[dict]
        List of dictionaries with metadata of files found in zip archive.
    """
    files_in_zip_lst = []
    # Select zip files only.
    zip_files = [f_meta for f_meta in files_metadata if f_meta.file_type == "zip"]
    logger.info(f"Number of zip files to scrap content from: {len(zip_files)}")
    # The Zenodo API does not provide any endpoint to get the content of zip files.
    # We use direct GET requests on the HTML preview of the zip files.
    for zip_counter, zip_file in enumerate(zip_files, start=1):
        url = (
            f"https://zenodo.org/records/{zip_file.dataset_id_in_repository}"
            f"/preview/{zip_file.file_name}"
        )
        files_tmp = extract_data_from_zip_file(
            url,
            logger=logger,
        )
        if not files_tmp:
            continue
        # Add common extra fields
        for file_meta in files_tmp:
            file_meta["dataset_repository_name"] = zip_file.dataset_repository_name
            file_meta["dataset_id_in_repository"] = zip_file.dataset_id_in_repository
            file_meta["dataset_url_in_repository"] = zip_file.dataset_url_in_repository
            file_meta["containing_archive_file_name"] = zip_file.file_name
            file_meta["file_url_in_repository"] = zip_file.file_url_in_repository
            files_in_zip_lst.append(file_meta)
        logger.info(
            "Zenodo zip files scraped: "
            f"{zip_counter}/{len(zip_files)} "
            f"({zip_counter / len(zip_files):.0%})"
        )
    return files_in_zip_lst


def extract_metadata_from_json(
    response_json: dict, logger: "loguru.Logger" = loguru.logger
) -> tuple[list[dict], list[dict]]:
    """Extract information from the Zenodo records.

    Arguments
    ---------
    response_json: dict
        JSON object obtained after a request on Zenodo API.

    Returns
    -------
    datasets: list[dict]
        List of datasets metadata.
    files: list[dict]
        List of files metadata.
    """
    datasets = []
    files = []
    try:
        _ = response_json["hits"]["hits"]
    except KeyError:
        logger.warning("Cannot extract hits from the response JSON.")
        return datasets, files
    for hit in response_json["hits"]["hits"]:
        # 'hit' is a Python dictionary.
        if hit.get("metadata", {}).get("access_right", "") != "open":
            continue
        dataset_id = str(hit["id"])
        logger.info(f"Extracting metadata for dataset id: {dataset_id}")
        dataset_dict = {
            "dataset_repository_name": DatasetSourceName.ZENODO,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": hit.get("links", {}).get("self_html", ""),
            "date_created": hit.get("created", None),
            "date_last_updated": hit.get("modified", None),
            "title": clean_text(hit.get("metadata", {}).get("title", "")),
            "author_names": [
                author.get("name")
                for author in hit.get("metadata", {}).get("creators", [])
                if author.get("name", None)
            ],
            "description": clean_text(hit.get("metadata", {}).get("description", "")),
            "keywords": [
                str(keyword) for keyword in hit.get("metadata", {}).get("keywords", [])
            ],
            "license": hit.get("metadata", {}).get("license", {}).get("id", None),
            "doi": hit.get("doi", None),
            "number_of_files": len(hit.get("files", [])),
            "download_number": hit.get("stats", {}).get("downloads", None),
            "view_number": hit.get("stats", {}).get("views", None),
        }
        datasets.append(dataset_dict)
        logger.info(f"Dataset URL: {dataset_dict['dataset_url_in_repository']}")
        for file_in in hit.get("files", []):
            file_dict = {
                "dataset_repository_name": dataset_dict["dataset_repository_name"],
                "dataset_id_in_repository": dataset_dict["dataset_id_in_repository"],
                "dataset_url_in_repository": dataset_dict["dataset_url_in_repository"],
                "file_name": file_in.get("key", ""),
                "file_url_in_repository": file_in.get("links", {}).get("self", ""),
                # File size in bytes.
                "file_size_in_bytes": file_in.get("size", None),
                "file_md5": file_in.get("checksum", "").removeprefix("md5:"),
                "containing_archive_file_name": None,
            }
            files.append(file_dict)
    return datasets, files


def search_zenodo(
    query: str,
    scraper: ScraperContext,
    page: int = 1,
    number_of_results: int = 1,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """Get total number of hits for a given query.

    Parameters
    ----------
    query : str
        The search query string.
    scraper: ScraperContext
        Scraper context manager containing configuration.
    page : int, optional
        The page number to retrieve. Default is 1.
    number_of_results : int, optional
        Number of results per page. Default is 1.
    logger : loguru.Logger, optional
        Logger for logging messages.

    Returns
    -------
    response_json : dict
        JSON response from the Zenodo API.
    """
    params = {
        "q": query,
        "size": number_of_results,
        "page": page,
        "status": "published",
        "access_token": scraper.token,
    }
    response_json = None
    response = make_http_get_request_with_retries(
        url="https://zenodo.org/api/records",
        params=params,
        timeout=60,
        logger=logger,
        delay_before_request=2,
        max_attempts=5,
    )
    if response is None:
        logger.warning("Failed to get response from the Zenodo API.")
        logger.warning("Getting next file type...")
        return None
    # Try to decode JSON response.
    try:
        response_json = response.json()
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.warning("Failed to decode JSON response from the Zenodo API.")
        logger.warning(f"Error: {exc}")
        return None
    # Try to extract hits (= results).
    try:
        _ = response_json["hits"]
        _ = int(response_json["hits"]["total"])
    except (KeyError, ValueError):
        logger.warning("Cannot extract hits for HTTP response.")
        logger.debug("Response JSON")
        logger.debug(response_json)
        return None
    return response_json


def search_all_datasets(
    file_types: list[dict],
    keywords: list[str],
    scraper: ScraperContext,
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[list[dict], list[dict]]:
    """Search all datasets on Zenodo.

    Parameters
    ----------
    file_types : list of dict
        List of file types to search for.
        Each dict contains:
        - type: str, file extension
        - keywords: str, "keywords" or "none"
    keywords : list of str
        List of keywords to use in the search.
    scraper: ScraperContext
        Context manager containing configuration and logger.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    datasets : list of dict
        List with datasets metadata.
    files : list of dict
        List with files metadata.
    """
    # There is a hard limit of the number of hits
    # one can get from a single query.
    max_hits_per_query = 10_000
    # We used paging with max_hits_per_page per page.
    max_hits_per_page = 100
    # Build query part with keywords. We want something like:
    # AND ("KEYWORD 1" OR "KEYWORD 2" OR "KEYWORD 3")
    query_keywords = ' AND ("' + '" OR "'.join(keywords) + '")'
    # Create empty lists to store results.
    datasets = []
    files = []
    logger.info("-" * 30)
    for file_type in file_types:
        logger.info(f"Looking for filetype: {file_type['type']}")
        datasets_count_old = len(datasets)
        # Build query with file type and optional keywords.
        query = f"""resource_type.type:"dataset" AND filetype:"{file_type["type"]}" """
        if file_type["keywords"] == "keywords":
            query += query_keywords
        logger.info("Query:")
        logger.info(f"{query}")
        # First, get the total number of hits for a given query.
        # This is needed to compute the number of pages of results to get.
        json_response = search_zenodo(
            query, scraper, page=1, number_of_results=1, logger=logger
        )
        if json_response is None or int(json_response["hits"]["total"]) == 0:
            logger.error("Getting next file type...")
            logger.info("-" * 30)
            continue
        total_hits = int(json_response["hits"]["total"])
        logger.info(f"Total hits for this query: {total_hits}")
        page_max = total_hits // max_hits_per_page + 1
        if scraper.is_in_debug_mode:
            logger.warning("Debug mode is ON")
            logger.warning("Limiting the number of pages to 1 with 10 hits per page.")
            page_max = 1
            max_hits_per_page = 10
        # Then, slice the query by page.
        for page in range(1, page_max + 1):
            logger.info(
                f"Starting page {page}/{page_max} for filetype: {file_type['type']}"
            )
            json_response = search_zenodo(
                query,
                scraper,
                page=page,
                number_of_results=max_hits_per_page,
                logger=logger,
            )
            if json_response is None:
                logger.warning("Failed to get response from the Zenodo API.")
                logger.warning("Getting next page...")
                continue
            datasets_tmp, files_tmp = extract_metadata_from_json(
                json_response, logger=logger
            )
            # Merge datasets and remove duplicates.
            datasets = remove_duplicates_in_list_of_dicts(datasets + datasets_tmp)
            # Merge files and remove duplicates.
            files = remove_duplicates_in_list_of_dicts(files + files_tmp)
            logger.success(
                f"Found so far: {len(datasets):,} datasets and {len(files):,} files"
            )
            if page * max_hits_per_page >= max_hits_per_query:
                logger.info("Max hits per query reached!")
                break
        logger.info(
            f"Number of datasets found: {len(datasets_tmp):,} "
            f"({len(datasets) - datasets_count_old} new)"
        )
        logger.info(f"Number of files found: {len(files_tmp):,}")
        logger.info("-" * 30)
    logger.info(f"Total number of datasets found: {len(datasets):,}")
    logger.info(f"Total number of files found: {len(files):,}")
    return datasets, files


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
    "--query-file",
    "query_file_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Query parameters file (YAML format).",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(
    output_dir_path: Path, query_file_path: Path, *, is_in_debug_mode: bool = False
) -> None:
    """Scrape Zenodo datasets and files."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.ZENODO,
        output_dir_path=output_dir_path,
        query_file_path=query_file_path,
        is_in_debug_mode=is_in_debug_mode,
    )
    logger = create_logger(logpath=scraper.log_file_path, level="INFO")
    # Log script name and doctring.
    logger.info(__file__)
    logger.info(__doc__)
    # Read and verify Zenodo token.
    load_dotenv()
    zenodo_token = os.environ.get("ZENODO_TOKEN", "")
    if not zenodo_token:
        logger.critical("No Zenodo token found.")
        logger.critical("Aborting.")
        sys.exit(1)
    else:
        logger.success("Found Zenodo token.")
        scraper.token = zenodo_token
    # Test connection to Zenodo API.
    if is_zenodo_connection_working(scraper.token, logger=logger):
        logger.success("Connection to Zenodo API successful.")
    else:
        logger.critical("Connection to Zenodo API failed.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Get rate limit information.
    get_rate_limit_info(
        [
            "https://zenodo.org/api/records",
            "https://zenodo.org/records/4444751/preview/code.zip",
        ],
        zenodo_token,
        logger=logger,
    )
    # Read parameter file
    (file_types, keywords, excluded_files, excluded_paths) = read_query_file(
        scraper.query_file_path,
        logger=logger,
    )
    datasets_metadata, files_metadata = search_all_datasets(
        file_types, keywords, scraper, logger=logger
    )
    # Normalize datasets and files metadata.
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_metadata, logger=logger
    )
    files_normalized_metadata = normalize_files_metadata(files_metadata, logger=logger)
    # Scrap zip files content.
    logger.info("-" * 30)
    files_zip_metadata = scrap_zip_content(files_normalized_metadata, logger=logger)
    logger.info(f"Number of files found inside zip files: {len(files_zip_metadata)}")
    # Normalize files metadata from zip files.
    zip_normalized_metadata = normalize_files_metadata(
        files_zip_metadata, logger=logger
    )
    # Merge all metadata files.
    files_normalized_metadata += zip_normalized_metadata
    logger.info(f"Total number of files found: {len(files_normalized_metadata)}")
    files_normalized_metadata = remove_excluded_files(
        files_normalized_metadata, excluded_files, excluded_paths
    )
    logger.info("-" * 30)

    # Remove datasets that contain non-MD related files
    # that come from zip files.
    datasets_normalized_metadata, files_normalized_metadata = (
        find_remove_false_positive_datasets(
            datasets_normalized_metadata,
            files_normalized_metadata,
            scraper,
            logger=logger,
        )
    )
    # Save metadata to parquet files.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_normalized_metadata,
        logger=logger,
    )
    scraper.number_of_files_scraped = export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_normalized_metadata,
        logger=logger,
    )
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()

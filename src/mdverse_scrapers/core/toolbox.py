"""Common functions and utilities used in the project."""

import argparse
import re
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx
import loguru
import pandas as pd
import yaml
from bs4 import BeautifulSoup

from ..models.dataset import DatasetMetadata
from ..models.file import FileMetadata
from ..models.scraper import ScraperContext

warnings.filterwarnings(
    "ignore",
    message="The input looks more like a filename than markup",
    category=UserWarning,
    module="bs4",
)


def make_http_get_request_with_retries(
    url: str,
    params: dict | None = None,
    timeout: int = 10,
    delay_before_request: int = 1,
    max_attempts: int = 3,
    logger: "loguru.Logger" = loguru.logger,
) -> httpx.Response | None:
    """Make HTTP GET request with retries on failure.

    Parameters
    ----------
    url : str
        The URL to send the GET request to.
    params : dict | None
        Optional dictionary of query parameters to include in the request.
    timeout : int
        Timeout for the HTTP request in seconds.
    max_attempts : int
        Maximum number of attempts to make.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    httpx.Response | None
        The HTTP response if successful, None otherwise.

    Raises
    ------
    httpx.HTTPStatusError
        If the request returns a 202 code,
        indicating the request is accepted but not ready yet.
        This error is caught and retried.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        ),
    }
    logger.info("Making HTTP GET request to:")
    logger.info(url)
    for attempt in range(1, max_attempts + 1):
        try:
            # Fist attempt, wait delay_before_request seconds,
            # Second attempt, wait delay_before_request + 10 seconds,
            # Third attempt, wait delay_before_request + 20 seconds, etc.
            time.sleep(delay_before_request + (attempt - 1) * 10)
            response = httpx.get(
                url,
                params=params,
                headers=headers,
                follow_redirects=True,
                timeout=timeout,
            )
            response.raise_for_status()
            # Raise an error if status code is 202,
            # indicating the request is accepted but not ready yet.
            if response.status_code == 202:
                msg = "Status code 202. Request accepted but not ready yet."
                raise httpx.HTTPStatusError(
                    msg, request=response.request, response=response
                )
            return response
        except httpx.TimeoutException:
            logger.warning(f"Attempt {attempt}/{max_attempts} timed out.")
            logger.warning(f"Timeout: {timeout} seconds.")
        except httpx.RequestError as exc:
            # httpx.RequestError only has a .request attribute.
            logger.warning(f"Attempt {attempt}/{max_attempts} failed.")
            logger.debug("Query headers:")
            logger.debug(exc.request.headers)
            logger.warning(f"Error details: {exc}")
        except httpx.HTTPStatusError as exc:
            # httpx.HTTPStatusError has .request and .response attributes.
            logger.warning(f"Attempt {attempt}/{max_attempts} failed.")
            logger.warning(f"Status code: {exc.response.status_code}")
            logger.debug("Query headers:")
            logger.debug(exc.request.headers)
            logger.debug("Response headers:")
            logger.debug(exc.response.headers)
        if attempt == max_attempts:
            logger.error(f"Maximum attempts ({max_attempts}) reached for URL:")
            logger.error(url)
            logger.error("Giving up!")
        else:
            logger.info("Retrying...")
    return None


def get_scraper_cli_arguments():
    """Parse scraper scripts command line.

    Returns
    -------
    argparse.ArgumentParser()
        Object with options
    """
    parser = argparse.ArgumentParser(add_help=False)
    required = parser.add_argument_group("required arguments")
    optional = parser.add_argument_group("optional arguments")
    required.add_argument(
        "--query",
        type=str,
        help="Query file (YAML format)",
        required=True,
    )
    required.add_argument(
        "--output-path",
        action="store",
        type=str,
        help="Directory path to save results",
        required=True,
    )
    # Add help.
    optional.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    return parser.parse_args()


def read_query_file(query_file_path: Path, logger: "loguru.Logger" = loguru.logger):
    """Read the query definition file.

    The query definition file is formatted in yaml.

    Parameters
    ----------
    query_file_path : Path
        Path to the query definition file.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    file_types : dict
        Dictionary with type, engine and keywords to use.
    keywords : list[str]
        Keywords related to molecular dynamics.
    exclusion_file_patterns : list[str]
        Patterns for files exclusion.
    exclusion_path_patterns : list[str]
        Patterns for path exclusion.
    """
    with open(query_file_path) as param_file:
        logger.info(f"Reading parameters from: {query_file_path}")
        data_loaded = yaml.safe_load(param_file)
    keywords = data_loaded["keywords"]
    file_types = data_loaded["file_types"]
    exclusion_file_patterns = data_loaded["excluded_files_starting_with"]
    exclusion_path_patterns = data_loaded["excluded_paths_containing"]
    logger.success(f"Found {len(file_types)} file types.")
    logger.success(f"Found {len(keywords)} keywords.")
    logger.success(f"Found {len(exclusion_file_patterns)} exclusion file patterns.")
    logger.success(f"Found {len(exclusion_path_patterns)} exclusion path patterns.")
    return file_types, keywords, exclusion_file_patterns, exclusion_path_patterns


def remove_duplicates_in_list_of_dicts(input_list: list[dict]) -> list[dict]:
    """Remove duplicates in a list while preserving the original order.

    Parameters
    ----------
    input_list : list
        List with possible duplicate entries.

    Returns
    -------
    list
        List without duplicates.
    """
    output_list = []
    for dict_item in input_list:
        if dict_item not in output_list:
            output_list.append(dict_item)
    return output_list


def clean_text(string):
    """Decode html and remove breaks.

    Arguments
    ---------
    string: str
        input string

    Returns
    -------
    str
        decoded string.
    """
    # Remove HTML tags
    # text_decode = BeautifulSoup(string, features="lxml")
    # text_decode = u''.join(text_decode.findAll(text=True))
    text_decode = BeautifulSoup(string, features="lxml").text
    # Remove tabulation and carriage return
    text_decode = re.sub(r"[\n\r\t]", " ", text_decode)
    # Remove multi spaces
    text_decode = re.sub(r" {2,}", " ", text_decode)
    return text_decode


def remove_excluded_files(
    files_metadata: list[FileMetadata],
    exclusion_file_patterns: list[str],
    exclusion_path_patterns: list[str],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """Remove excluded files.

    Excluded files are, for example:
    - Files whose name starts with a .
    - Files whose path contains .git or __MACOSX

    Parameters
    ----------
    files_metadata : list[FileMetadata]
        List of files metadata.
    exclusion_file_patterns : list[str]
        Patterns for file exclusion.
    exclusion_path_patterns : list[str]
        Patterns for path exclusion.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[FileMetadata]
        List of files metadata without excluded files and paths.
    """
    excluded_files_count = {}
    files_remaining = []
    for file_meta in files_metadata:
        is_excluded = False
        # Search exclusion patterns in file path.
        for pattern in exclusion_path_patterns:
            if pattern in file_meta.file_name:
                pattern_label = f"in path: {pattern}"
                excluded_files_count[pattern_label] = (
                    excluded_files_count.get(pattern_label, 0) + 1
                )
                is_excluded = True
                break
        # Don't check file name patterns if already excluded by path.
        if is_excluded:
            continue
        # Search exclusion patterns in file name.
        name = file_meta.file_name.split("/")[-1]
        for pattern in exclusion_file_patterns:
            if name.startswith(pattern):
                pattern_label = f"starting with: {pattern}"
                excluded_files_count[pattern_label] = (
                    excluded_files_count.get(pattern_label, 0) + 1
                )
                is_excluded = True
                break
        if not is_excluded:
            files_remaining.append(file_meta)
    logger.info(f"Removed {len(files_metadata) - len(files_remaining)} excluded files")
    for pattern_label, count in excluded_files_count.items():
        logger.info(f"- {count} files excluded for pattern -> {pattern_label}")
    logger.info(f"Remaining files: {len(files_remaining)}")
    return files_remaining


def find_false_positive_datasets(
    files_metadata: list[FileMetadata],
    md_file_types: list[str],
    logger: "loguru.Logger" = loguru.logger,
) -> list[str]:
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.

    Parameters
    ----------
    files_metadata : list[FileMetadata]
        List of files metadata.
    md_file_types: list[str]
        List of molecular dynamics file types.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[str]
        List of false positive dataset ids.
    """
    # Get total number of files and unique file types per dataset.
    files_df = pd.DataFrame([file_meta.model_dump() for file_meta in files_metadata])
    unique_file_types_per_dataset = (
        files_df.groupby("dataset_id_in_repository")["file_type"]
        .agg(["count", "unique"])
        .rename(columns={"count": "total_files", "unique": "unique_file_types"})
        .sort_values(by="total_files", ascending=False)
    )
    false_positive_datasets = []
    for dataset_id in unique_file_types_per_dataset.index:
        dataset_file_types = list(
            unique_file_types_per_dataset.loc[dataset_id, "unique_file_types"]
        )
        number_of_files = unique_file_types_per_dataset.loc[dataset_id, "total_files"]
        dataset_url = files_df.query(
            f"dataset_id_in_repository == '{dataset_id}'"
        ).iloc[0]["dataset_url_in_repository"]
        # For a given dataset, if there is no MD file types in the entire set
        # of the dataset file types, then we probably have a false-positive dataset,
        # i.e. a dataset that does not contain any MD data.
        # We print the total number of files in the dataset
        # and the first 20 file types for extra verification.
        if len(set(dataset_file_types) & set(md_file_types)) == 0:
            logger.info(f"Dataset {dataset_id} is probably a false positive:")
            logger.info(dataset_url)
            logger.info(f"Dataset will be removed with its {number_of_files} files.")
            logger.info("List of the first file types:")
            logger.info(" ".join(dataset_file_types[:20]))
            logger.info("-" * 30)
            false_positive_datasets.append(dataset_id)
    logger.info(
        f"In total, {len(false_positive_datasets):,} "
        "false positive datasets will be removed."
    )
    logger.info("-" * 30)
    return false_positive_datasets


def remove_false_positive_datasets(
    metadata: list[DatasetMetadata] | list[FileMetadata],
    dataset_ids_to_remove: list[str],
    logger: "loguru.Logger" = loguru.logger,
) -> list[DatasetMetadata] | list[FileMetadata]:
    """Remove false positive datasets from datasets or files metadata.

    Parameters
    ----------
    metadata : list[DatasetMetadata] | list[FileMetadata]
        List of metadata to clean (datasets or files).
    dataset_ids_to_remove : list[str]
        List of dataset ids to remove.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[DatasetMetadata] | list[FileMetadata]
        Cleaned metadata.
    """
    metadata_clean = [
        meta
        for meta in metadata
        if meta.dataset_id_in_repository not in dataset_ids_to_remove
    ]
    logger.info(f"Removed: {len(metadata) - len(metadata_clean):,}")
    logger.info(f"Remaining: {len(metadata_clean):,}")
    return metadata_clean


def find_remove_false_positive_datasets(
    datasets_metadata: list[DatasetMetadata],
    files_metadata: list[FileMetadata],
    scraper: ScraperContext,
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[list[DatasetMetadata], list[FileMetadata]]:
    """Find and remove false-positive datasets.

    False-positive datasets do not contain MD-related files.

    Parameters
    ----------
    datasets_metadata : list[DatasetMetadata]
        List of datasets metadata.
    files_metadata : list[FileMetadata]
        List of files metadata.
    scraper : ScraperContext
        ScraperContext object.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    tuple[list[DatasetMetadata], list[FileMetadata]]
        Cleaned lists of metadata for datasets and files.
    """
    # Read parameter file.
    file_types, _, _, _ = read_query_file(scraper.query_file_path, logger=logger)
    # List file types from the query parameter file.
    file_types_lst = [file_type["type"] for file_type in file_types]
    # Zip is not a MD-specific file type.
    file_types_lst.remove("zip")
    # Find false-positive datasets.
    false_positive_datasets = find_false_positive_datasets(
        files_metadata, file_types_lst, logger=logger
    )
    # Remove false-positive datasets from all dataframes.
    logger.info("Removing false-positive datasets in datasets...")
    datasets_metadata = remove_false_positive_datasets(
        datasets_metadata, false_positive_datasets, logger=logger
    )
    logger.info("Removing false-positive datasets in files...")
    files_metadata = remove_false_positive_datasets(
        files_metadata, false_positive_datasets, logger=logger
    )
    return datasets_metadata, files_metadata


def validate_http_url(v: str) -> str:
    """
    Validate that the input string is a reachable HTTP or HTTPS URL.

    Parameters
    ----------
    v : str
        The input string to validate as a URL.

    Returns
    -------
    str
        The validated URL, if it is well-formed and reachable.

    Raises
    ------
    ValueError
        If the URL is not well-formed or not reachable.
    """
    parsed = urlparse(v)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        msg = f"Invalid URL format: {v}"
        raise ValueError(msg)

    try:
        # Perform a HEAD request (faster+lighter than GET)
        response = httpx.head(v, timeout=5.0)
        if response.status_code >= 400:
            msg = f"URL not reachable (status code {response.status_code}): {v}"
            raise ValueError(msg)
    except httpx.RequestError as e:
        msg = f"Failed to connect to URL {v}: {e}"
        raise ValueError(msg) from e

    return v


def format_date(date: datetime | str) -> str:
    """Convert datetime objects or ISO strings to '%Y-%m-%dT%H:%M:%S' format.

    Parameters
    ----------
    date : str
        The date to validate the format.

    Returns
    -------
    str:
        The date in '%Y-%m-%dT%H:%M:%S' format.

    Raises
    ------
    ValueError
        If the input string is not in a valid ISO 8601 format.
    TypeError
        If the input is neither a datetime object nor a string.
    """
    if isinstance(date, datetime):
        # Ensure formatting consistency by re-parsing the formatted string
        return date.strftime("%Y-%m-%dT%H:%M:%S")

    if isinstance(date, str):
        try:
            dt = datetime.fromisoformat(date)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError as err:
            msg = (
                f"Invalid datetime format: {date}. Expected format: YYYY-MM-DDTHH:MM:SS"
            )
            raise ValueError(msg) from err
    msg = f"Expected datetime or str, got {type(date).__name__}"
    raise TypeError(msg)


def convert_file_size_to_human_readable(size_in_bytes: float) -> str:
    """Convert file size in bytes to a human-readable format.

    Parameters
    ----------
    size_in_bytes : float
        File size in bytes.

    Returns
    -------
    str
        File size in a human-readable format (e.g., '10.52 MB').
    """
    if size_in_bytes < 0:
        return "Negative size!"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_in_bytes < 1000.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1000.0
    return "File too big!"


def print_statistics(
    scraper: ScraperContext, logger: "loguru.Logger" = loguru.logger
) -> None:
    """Print scraping statistics.

    Parameters
    ----------
    scraper : ScraperContext
        Context of the scraper.
    logger: "loguru.Logger"
        Logger for logging messages.
    """
    logger.info("-" * 30)
    # Print statistics for datasets.
    logger.success(
        f"Number of datasets scraped: {scraper.number_of_datasets_scraped:,}"
    )
    if not scraper.datasets_parquet_file_path.is_file():
        logger.error("Datasets parquet file not found!")
        logger.error(f"{scraper.datasets_parquet_file_path} is missing.")
    else:
        datasets_parquet_size = convert_file_size_to_human_readable(
            scraper.datasets_parquet_file_path.stat().st_size
        )
        logger.info(
            f"Saved in: {scraper.datasets_parquet_file_path} ({datasets_parquet_size})"
        )
    # Print statistics for files.
    logger.success(f"Number of files scraped: {scraper.number_of_files_scraped:,}")
    if not scraper.files_parquet_file_path.is_file():
        logger.error("Files parquet file not found!")
        logger.error(f"{scraper.files_parquet_file_path} is missing.")
    else:
        files_parquet_size = convert_file_size_to_human_readable(
            scraper.files_parquet_file_path.stat().st_size
        )
        logger.info(
            f"Saved in: {scraper.files_parquet_file_path} ({files_parquet_size})"
        )
    # Print elapsed time.
    elapsed_time = int((datetime.now() - scraper.start_time).total_seconds())
    logger.success(
        f"Scraped {scraper.data_source_name} in: {timedelta(seconds=elapsed_time)} 🎉"
    )
    # Print where log file is saved.
    logger.info(f"Saved log file in: {scraper.log_file_path}")
    if scraper.is_in_debug_mode:
        logger.warning("---Debug mode was ON---")

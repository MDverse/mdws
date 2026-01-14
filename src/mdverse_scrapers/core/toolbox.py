"""Common functions and utilities used in the project."""

import argparse
import pathlib
import re
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import loguru
import pandas as pd
import yaml
from bs4 import BeautifulSoup

from ..models.enums import DataType

warnings.filterwarnings(
    "ignore",
    message="The input looks more like a filename than markup",
    category=UserWarning,
    module="bs4",
)

@dataclass(kw_only=True)
class ContextManager:
    """ContextManager dataclass."""

    logger: "loguru.Logger" = loguru.logger
    output_path: pathlib.Path = pathlib.Path("")
    query_file_name: pathlib.Path = pathlib.Path("")
    token: str = ""


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
                    msg,
                    request=response.request,
                    response=response
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


def read_query_file(query_file_path, logger: "loguru.Logger" = loguru.logger):
    """Read the query definition file.

    The query definition file is formatted in yaml.

    Parameters
    ----------
    query_file_path : str
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


def verify_file_exists(filename):
    """Verify file exists.

    Parameters
    ----------
    filename : str
        Name of file to verify existence

    Raises
    ------
    FileNotFoundError
        If file does not exist or is not a file.
    """
    file_in = pathlib.Path(filename)
    if not file_in.exists():
        msg = f"File {filename} not found"
        raise FileNotFoundError(msg)
    if not file_in.is_file():
        msg = f"{filename} is not a file"
        raise FileNotFoundError(msg)


def verify_output_directory(directory, logger: "loguru.Logger" = loguru.logger):
    """Verify output directory exists.

    Create it if necessary.

    Parameters
    ----------
    directory : str
        Path to directory to store results
    logger : "loguru.Logger"
        Logger for logging messages.

    Raises
    ------
    FileNotFoundError
        If directory path is an existing file.
    """
    directory_path = pathlib.Path(directory)
    if directory_path.is_file():
        msg = f"{directory} is an existing file."
        raise FileNotFoundError(msg)
    if directory_path.is_dir():
        logger.info(f"Output directory {directory} already exists.")
    else:
        directory_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created output directory {directory}")


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


def extract_file_extension(file_path: str) -> str:
    """Extract file extension from file path.

    Parameters
    ----------
    file_path : str
        File path
        Example: "/something/here/file.txt"

    Returns
    -------
    str
        File extension without a dot.
        Example: "txt"
    """
    # Extract the file name for its path.
    file_name = file_path.split("/")[-1]
    file_type = "none"
    if "." in file_name:
        file_type = file_name.split(".")[-1].lower()
    return file_type


def extract_date(date_str):
    """Extract and format date from a string.

    Parameters
    ----------
    date_str : str
        Date as a string in ISO 8601.
        For example: 2020-07-29T19:22:57.752335+00:00

    Returns
    -------
    str
        Date as in string in YYYY-MM-DD format.
        For example: 2020-07-29
    """
    date = datetime.fromisoformat(date_str)
    return f"{date:%Y-%m-%d}"


def remove_excluded_files(
    files_df: pd.DataFrame,
    exclusion_file_patterns: list[str],
    exclusion_path_patterns: list[str],
) -> pd.DataFrame:
    """Remove excluded files.

    Excluded files are, for example:
    - Files whose name starts with a .
    - Files whose path contains .git or __MACOSX

    Parameters
    ----------
    files_df : Pandas dataframe
        Pandas dataframe with files metadata.
    exclusion_file_patterns : list
        Patterns for file exclusion.
    exclusion_path_patterns : list
        Patterns for path exclusion.

    Returns
    -------
    Pandas dataframe
        Dataframe without excluded files and paths.
    """
    df_tmp = files_df.copy(deep=True)
    # For file names with path, extract file name only:
    df_tmp["name"] = df_tmp["file_name"].apply(lambda x: x.split("/")[-1])

    boolean_mask = pd.Series(data=False, index=files_df.index)
    print("-" * 30)

    for pattern in exclusion_path_patterns:
        print(f"Selecting file paths containing: {pattern}")
        selection = df_tmp["file_name"].str.contains(pat=pattern, regex=False)
        print(f"Found {sum(selection)} files")
        boolean_mask = boolean_mask | selection

    for pattern in exclusion_file_patterns:
        print(f"Selecting file names starting with: {pattern}")
        selection = df_tmp["name"].str.startswith(pattern)
        print(f"Found {sum(selection)} files")
        boolean_mask = boolean_mask | selection

    print(f"Removed {sum(boolean_mask)} excluded files")
    print(f"Remaining files: {sum(~boolean_mask)}")
    print("-" * 30)
    return files_df[~boolean_mask]


def find_false_positive_datasets(
    files_df: pd.DataFrame,
    md_file_types: list[str],
    logger: "loguru.Logger" = loguru.logger,
) -> list[str]:
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.

    Parameters
    ----------
    files_df : pd.DataFrame
        Dataframe which contains all files metadata from a given repo.
    md_file_types: list
        List containing molecular dynamics file types.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list
        List of false positive dataset ids.
    """
    # Get total number of files and unique file types per dataset.
    unique_file_types_per_dataset = (
        files_df.groupby("dataset_id")["file_type"]
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
        dataset_url = files_df.query(f"dataset_id == '{dataset_id}'").iloc[0][
            "dataset_url"
        ]
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
            logger.info("---")
            false_positive_datasets.append(dataset_id)
    logger.info(
        f"In total, {len(false_positive_datasets):,} false positive datasets "
        "will be removed."
    )
    logger.info("---")
    return false_positive_datasets


def remove_false_positive_datasets(
    df_to_clean: pd.DataFrame,
    dataset_ids_to_remove: list[str],
    logger: "loguru.Logger" = loguru.logger,
) -> pd.DataFrame:
    """Remove false positive datasets from file.

    Parameters
    ----------
    df_to_clean : pd.DataFrame
        Dataframe to clean.
    dataset_ids_to_remove : list
        List of dataset ids to remove.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe.
    """
    records_count_old = len(df_to_clean)
    # We keep rows NOT associated to false-positive dataset ids
    df_clean = df_to_clean[~df_to_clean["dataset_id"].isin(dataset_ids_to_remove)]
    records_count_clean = len(df_clean)
    logger.info(
        f"Removing {records_count_old - records_count_clean:,} lines "
        f"({records_count_old:,} -> {records_count_clean:,}) in dataframe."
    )
    return df_clean


def find_remove_false_positive_datasets(
    datasets_df: pd.DataFrame,
    files_df: pd.DataFrame,
    ctx: ContextManager,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Find and remove false-positive datasets.

    False-positive datasets do not contain MD-related files.

    Parameters
    ----------
    datasets_df : pd.DataFrame
        Dataframe with information about datasets.
    files_df : pd.DataFrame
        Dataframe with information about files.
    ctx : toolbox.ContextManager
        ContextManager object.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Cleaned dataframes for:
        - datasets
        - files
    """
    # Read parameter file.
    file_types, _, _, _ = read_query_file(ctx.query_file_name)
    # List file types from the query parameter file.
    file_types_lst = [file_type["type"] for file_type in file_types]
    # Zip is not a MD-specific file type.
    file_types_lst.remove("zip")
    # Find false-positive datasets.
    false_positive_datasets = find_false_positive_datasets(
        files_df, file_types_lst, logger=ctx.logger
    )
    # Remove false-positive datasets from all dataframes.
    ctx.logger.info("Removing false-positive datasets in the datasets dataframe...")
    datasets_df = remove_false_positive_datasets(
        datasets_df, false_positive_datasets, logger=ctx.logger
    )
    ctx.logger.info("Removing false-positive datasets in the files dataframe...")
    files_df = remove_false_positive_datasets(
        files_df, false_positive_datasets, logger=ctx.logger
    )
    return datasets_df, files_df


def export_dataframe_to_parquet(
    repository_name: str, suffix: DataType, df: pd.DataFrame, ctx: ContextManager
) -> None:
    """Export dataframes to parquet file.

    Parameters
    ----------
    repository_name : str
        Name of the data repository.
    suffix : DataType
        Suffix for the parquet file name.
    df : pd.DataFrame
        Dataframe to export.
    ctx : ContextManager
        ContextManager object.
    """
    parquet_name = ctx.output_path / f"{repository_name}_{suffix}.parquet"
    df.to_parquet(parquet_name, index=False)
    ctx.logger.success(f"Dataframe with {len(df):,} rows exported to:")
    ctx.logger.success(parquet_name)


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


def ensure_dir(ctx, param, value: Path) -> Path:
    """
    Create the directory if it does not already exist.

    Callback for Click options to ensure the provided path
    is a valid directory. Behaves like `mkdir -p`.

    Parameters
    ----------
    ctx : click.Context
        The Click context for the current command invocation.
        (Required by Click callbacks but unused in this function.)
    param : click.Parameter
        The Click parameter associated with this callback.
        (Required by Click callbacks but unused in this function.)
    value : Path
        The directory path provided by the user, already converted
        into a `pathlib.Path` object by Click.

    Returns
    -------
    Path
        The same path, after ensuring the directory exists.
    """
    value.mkdir(parents=True, exist_ok=True)
    return value

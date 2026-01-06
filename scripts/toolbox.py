"""Common functions and utilities used in the project."""

import argparse
import pathlib
import re
import warnings
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
import httpx
import pandas as pd
import yaml
from bs4 import BeautifulSoup

warnings.filterwarnings(
    "ignore",
    message="The input looks more like a filename than markup",
    category=UserWarning,
    module="bs4",
)



class DatasetRepository(StrEnum):
    """Supported repositories from which molecular dynamics datasets are scraped."""

    ZENODO = "ZENODO"
    FIGSHARE = "FIGSHARE"
    OSF = "OSF"
    NOMAD = "NOMAD"
    ATLAS = "ATLAS"
    GPCRMD = "GPCRMD"


class DatasetProject(StrEnum):
    """Supported projects from which molecular dynamics datasets are scraped."""

    ZENODO = "ZENODO"
    FIGSHARE = "FIGSHARE"
    OSF = "OSF"
    NOMAD = "NOMAD"
    ATLAS = "ATLAS"
    GPCRMD = "GPCRMD"


def load_token() -> None:
    """Load API token from .env file."""
    load_dotenv()


def load_database(filename, database_type):
    """Load datasets database.

    Parameters
    ----------
    filename : str
        Path to the database CVS file.
    database : str
        Type of database ("datasets", "texts" or "files").

    Returns
    -------
    pd.DataFrame
        Datasets in a Pandas dataframe.
    """
    df = pd.DataFrame()
    if database_type == "datasets" or database_type == "texts":
        df = pd.read_csv(filename, sep="\t", dtype={"dataset_id": str})
    elif database_type == "files":
        df = pd.read_csv(
            filename,
            sep="\t",
            dtype={
                "dataset_id": str,
                "file_type": str,
                "file_md5": str,
                "file_url": str,
            },
        )
    return df


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
        "--output",
        action="store",
        type=str,
        help="Path to save results",
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


def read_query_file(query_file_path):
    """Read the query definition file.

    The query definition file is formatted in yaml.

    Parameters
    ----------
    query_file_path : str
        Path to the query definition file.

    Returns
    -------
    file_types : dict
        Dictionary with type, engine and keywords to use.
    keywords : list
        Keywords related to molecular dynamics.
    exclusion_files : list
        Patterns for files exclusion.
    exclusion_paths : list
        Patterns for path exclusion.
    """
    with open(query_file_path) as param_file:
        print(f"Reading parameters from: {query_file_path}")
        data_loaded = yaml.safe_load(param_file)
    keywords = data_loaded["keywords"]
    file_types = data_loaded["file_types"]
    exclusion_files = data_loaded["excluded_files_starting_with"]
    exclusion_paths = data_loaded["excluded_paths_containing"]
    return file_types, keywords, exclusion_files, exclusion_paths


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


def verify_output_directory(directory):
    """Verify output directory exists.

    Create it if necessary.

    Parameters
    ----------
    directory : str
        Path to directory to store results

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
        msg = f"Output directory {directory} already exists."
        print(msg)
    else:
        directory_path.mkdir(parents=True, exist_ok=True)
        print(f"Created output directory {directory}")


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


def extract_file_extension(file_path):
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


def remove_excluded_files(files_df: pd.DataFrame, exclusion_file_patterns: list[str], exclusion_path_patterns: list[str]) -> pd.DataFrame:
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


def find_false_positive_datasets(files_df: pd.DataFrame, md_file_types: list[str]) -> list[str]:
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.

    Parameters
    ----------
    files_df : pd.DataFrame
        Dataframe which contains all files metadata from a given repo.
    md_file_types: list
        List containing molecular dynamics file types.

    Returns
    -------
    list
        List of false positive dataset ids.
    """
    # Get total number of files and unique file types per dataset.
    unique_file_types_per_dataset = (files_df
        .groupby("dataset_id")["file_type"]
        .agg(["count", "unique"])
        .rename(columns={"count": "total_files", "unique": "unique_file_types"})
        .sort_values(by="total_files", ascending=False)
    )
    false_positive_datasets = []
    for dataset_id in unique_file_types_per_dataset.index:
        dataset_file_types = list(
            unique_file_types_per_dataset.loc[dataset_id, "unique_file_types"]
        )
        number_files = unique_file_types_per_dataset.loc[dataset_id, "total_files"]
        dataset_url = files_df.query(f"dataset_id == '{dataset_id}'").iloc[0]["dataset_url"]
        # For a given dataset, if there is no MD file types in the entire set
        # of the dataset file types, then we probably have a false-positive dataset,
        # i.e. a dataset that does not contain any MD data.
        # We print the total number of files in the dataset
        # and the first 20 file types for extra verification.
        if len(set(dataset_file_types) & set(md_file_types)) == 0:
            print(f"Dataset {dataset_id} is probably a false positive:")
            print(dataset_url)
            print(
                f"Dataset will be removed with its {number_files} files."
            )
            print("List of the first file types:")
            print(" ".join(dataset_file_types[:20]))
            print("---")
            false_positive_datasets.append(dataset_id)
    print(f"In total, {len(false_positive_datasets)} false positive datasets will be removed.")
    print("---")
    return false_positive_datasets


def remove_false_positive_datasets(df_to_clean: pd.DataFrame, dataset_ids_to_remove: list[str]) -> pd.DataFrame:
    """Remove false positive datasets from file.

    Parameters
    ----------
    df_to_clean : pd.DataFrame
        Dataframe to clean.
    dataset_ids_to_remove : list
        List of dataset ids to remove.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe.
    """
    records_count_old = len(df_to_clean)
    # We keep rows NOT associated to false-positive dataset ids
    df_clean = df_to_clean[~df_to_clean["dataset_id"].isin(dataset_ids_to_remove)]
    records_count_clean = len(df_clean)
    print(
        f"Removing {records_count_old - records_count_clean} lines "
        f"({records_count_old} -> {records_count_clean}) in dataframe."
    )
    return df_clean


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
                f"Invalid datetime format: {date}. Expected format: "
                "YYYY-MM-DDTHH:MM:SS"
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

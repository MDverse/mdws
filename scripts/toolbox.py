"""Common functions and utilities used in the project."""

import argparse
from datetime import datetime
import logging
import pathlib
import re
import warnings

from bs4 import BeautifulSoup
import pandas as pd
import yaml


warnings.filterwarnings(
    "ignore",
    message="The input looks more like a filename than markup",
    category=UserWarning,
    module="bs4",
)


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
    if database_type == "datasets":
        df = pd.read_csv(
            filename,
            sep="\t",
            dtype={"dataset_id": str}
        )
    elif database_type == "texts":
        df = pd.read_csv(
            filename,
            sep="\t",
            dtype={"dataset_id": str}
        )
    elif database_type == "files":
        df = pd.read_csv(
            filename,
            sep="\t",
            dtype={"dataset_id": str, "file_type": str,
                   "file_md5": str, "file_url": str}
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
    with open(query_file_path, "r") as param_file:
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
    """
    file_in = pathlib.Path(filename)
    if not file_in.exists():
        raise FileNotFoundError(f"File {filename} not found")
    if not file_in.is_file():
        raise FileNotFoundError(f"File {filename} is not a file")


def verify_output_directory(directory):
    """Verify output directory exists.

    Create it if necessary.

    Parameters
    ----------
    directory : str
        Path to directory to store results
    """
    directory_path = pathlib.Path(directory)
    if directory_path.is_file():
        raise FileNotFoundError(f"{directory} is an existing file.")
    if directory_path.is_dir():
        print(f"Output directory {directory} already exists.")
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
    text_decode = re.sub(" {2,}", " ", text_decode)
    return text_decode


def extract_file_extension(file_path):
    """Extract file extension from file path.

    Parameters
    ----------
    file_path : str
        File path.
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


def remove_excluded_files(files_df, exclusion_files, exclusion_paths):
    """Remove excluded files.

    Excluded files are, for example:
    - Files whose name starts with a .
    - Files whose path contains .git or __MACOSX

    Parameters
    ----------
    files_df : Pandas dataframe
        Pandas dataframe with files description.
    exclusion_files : list
        Patterns for file exclusion.
    exclusion_paths : list
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

    for pattern in exclusion_paths:
        print(f"Selecting file paths containing: {pattern}")
        selection = df_tmp["file_name"].str.contains(pat=pattern, regex=False)
        print(f"Found {sum(selection)} files")
        boolean_mask = boolean_mask | selection

    for pattern in exclusion_files:
        print(f"Selecting file names starting with: {pattern}")
        selection = df_tmp["name"].str.startswith(pattern)
        print(f"Found {sum(selection)} files")
        boolean_mask = boolean_mask | selection

    print(f"Removed {sum(boolean_mask)} excluded files")
    print(f"Remaining files: {sum(~boolean_mask)}")
    print("-" * 30)
    return files_df[~boolean_mask]


def find_false_positive_datasets(files_filename, datasets_filename, md_file_types):
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.

    Parameters
    ----------
    files_filename : str
        Path to the file which contains all files from a given repo.
    datasets_filename : str
        Path to the file which contains all datasets from a given repo.
    md_file_types: list
        List containing molecular dynamics file types.

    Returns
    -------
    list
        List of false positive dataset ids.
    """
    files_df = load_database(files_filename, "files")
    datasets_df = load_database(datasets_filename, "datasets")
    # Merge datasets and files dataframes to get dataset url
    # together with file types.
    df = pd.merge(
        files_df, datasets_df,
        how="left",
        on=["dataset_id", "dataset_origin"],
        validate="many_to_one"
    )
    # Get total number of files and unique file types per dataset.
    unique_file_types_per_dataset = (
    df
        .groupby("dataset_id")["file_type"]
        .agg(["count", "unique"])
        .rename(columns={"count": "total_files", "unique": "unique_file_types"})
        .sort_values(by="total_files", ascending=False)
    )
    false_positives = []
    for dataset_id in unique_file_types_per_dataset.index:
        file_types = list(unique_file_types_per_dataset.loc[dataset_id, "unique_file_types"])
        number_files = unique_file_types_per_dataset.loc[dataset_id, "total_files"]
        dataset_url = (
        df
            .query(f"dataset_id == '{dataset_id}'")
            .iloc[0]["dataset_url"]
        )
        # Datasets that only contain zip files might have not been properly
        # parsed by the scrapper or zip preview was not available.
        # We remove these datasets.
        # if file_types == ["zip"]:
        #     print(f"Dataset {dataset_id} ({dataset_url})")
        #     print("contains only zip files -> keep dataset")
        #     print("---")
        #     continue
        # For a given dataset, if there is no MD file types in the entire set
        # of the dataset file types, then we probably have a false-positive dataset,
        # i.e. a dataset that does not contain any MD data.
        # We print the total number of files in the dataset
        # and the first 20 file types for extra verification.
        if len(set(file_types) & set(md_file_types)) == 0:
            print(f"Dataset {dataset_id} ({dataset_url}) is probably a false positive")
            print(f"Dataset {dataset_id} will be removed with its {number_files} files)")
            print(f"List of the first file types:")
            print(" ".join(file_types[:20]))
            print("---")
            false_positives.append(dataset_id)
    print(f"In total, {len(false_positives)} false positive datasets will be removed")
    print("---")
    return false_positives


def remove_false_positive_datasets(filename, database_type, dataset_ids_to_remove):
    """Remove false positive datasets from file.

    Parameters
    ----------
    filename : str
        Path of the file to clean.
    database_type : str
        Type of database ("datasets", "texts" or "files").
    dataset_ids_to_remove : list
        List of dataset ids to remove.
    """
    df = load_database(filename, database_type)
    records_count_old = len(df)
    # We keep rows NOT associated to false-positive dataset ids
    df_clean = df[~df["dataset_id"].isin(dataset_ids_to_remove)]
    records_count_clean = len(df_clean)
    print(f"Removing {records_count_old - records_count_clean} lines "
          f"({records_count_old} -> {records_count_clean}) in {filename}")
    df_clean.to_csv(filename, sep="\t", index=False)

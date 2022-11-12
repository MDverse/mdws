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


# Rewire the print function to logging.info
print = logging.info


warnings.filterwarnings(
    "ignore",
    message="The input looks more like a filename than markup",
    category=UserWarning,
    module="bs4",
)


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
    """Read the query definition file

    The query definition file is formatted in yaml.

    Parameters
    ----------
    query_file_path : str
        Path to the query definition file.

    Returns
    -------
    file_types : dict
        Dictionary with type, engine and keywords to use.
    md_keywords : list
        Keywords related to molecular dynamics.
    generic_keywords : list
        Generic keywords for zip archives.
    exclusion_files : list
        Patterns for files exclusion.
    exclusion_paths : list
        Patterns for path exclusion.
    """
    with open(query_file_path, "r") as param_file:
        print(f"Reading parameters from: {query_file_path}")
        data_loaded = yaml.safe_load(param_file)
    md_keywords = data_loaded["md_keywords"]
    generic_keywords = data_loaded["generic_keywords"]
    file_types = data_loaded["file_types"]
    exclusion_files = data_loaded["excluded_files_starting_with"]
    exclusion_paths = data_loaded["excluded_paths_containing"]
    return file_types, md_keywords, generic_keywords, exclusion_files, exclusion_paths


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
    """Decodes from html and removes breaks

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
    return files_df[~boolean_mask]


def read_md_files(filename):
    """Read MD files definition file.

    Parameters
    ----------
    filename : str
        Path to the MD files definition file.

    Returns
    -------
    list
        List of MD file types without zip.
    """
    with open(filename, "r") as filetypes_file:
        data_loaded = yaml.safe_load(filetypes_file)
    md_files = data_loaded["file_types"]
    md_types = [extension["type"] for extension in md_files]
    print(f"Found {len(md_types)} MD file types")
    return md_types


def find_false_positive_datasets(filename, md_file_types):
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.
    
    Parameters
    ----------
    filename : str
        Path to the file which contains all files from a given repo.
    md_file_types: list
        List containing molecular dynamics file types.

    Returns
    -------
    list
        Dictionary of false positive datasets
    """
    df = pd.read_csv(filename, sep="\t")
    df["file_type"] = df["file_type"].astype(str)
    unique_file_types_per_dataset = (df
        .groupby("dataset_id")["file_type"]
        .agg(["count", "unique"])
        .sort_values(by="count", ascending=False)
    )
    false_positives = []
    for index in unique_file_types_per_dataset.index:
        file_types = list(unique_file_types_per_dataset.loc[index, "unique"])
        number_files = unique_file_types_per_dataset.loc[index, "count"]
        # Datasets that only contain zip files might have not been properly
        # parsed by the scrapper or zip preview is not available.
        # In case of doubt, we keep these datasets.
        if file_types == ["zip"]:
            print(f"Dataset {index} contains only zip files -> keep")
            continue
        # For a fiven dataset, if there is no MD file types in the entire set 
        # of the dataset file types, then we might have a false-positive dataset.
        # We print the total number of files in the dataset
        # and the first 20 file types for extra verification.
        if len(set(file_types) & set(md_file_types)) == 0:
            print(f"Dataset {index} might be a false positive ({number_files} files)")
            print(" ".join(file_types[:20]))
            print("---")
            false_positives.append(index)
    return false_positives


def remove_false_positive_datasets(filename, dataset_ids_to_remove):
    """Remove false positive datasets from file.

    Parameters
    ----------
    filename : str
        Path to the data file
    dataset_ids_to_remove : list
        List of dataset ids to remove
    """
    df = pd.read_csv(filename, sep="\t")
    records_count_old = df.shape[0]
    # We keep rows NOT associated to false-positive dataset ids
    df_clean = df[~df["dataset_id"].isin(dataset_ids_to_remove)]
    records_count_clean = df_clean.shape[0]
    print(f"Removing {records_count_old - records_count_clean} lines "
          f"({records_count_old} -> {records_count_clean}) : {filename}")
    df_clean.to_csv(filename, sep="\t", index=False)

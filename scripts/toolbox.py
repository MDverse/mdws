"""Common functions and utilities used in the project."""

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

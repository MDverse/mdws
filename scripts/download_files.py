"""Download files.

We use Pooch, a Python library dedicated to fetch data files
https://www.fatiando.org/pooch/latest/index.html

Pooch can download a file and verify its integrity with a given hash.
It also uses a local cache and downloads data once.
"""

import argparse
import logging
import pathlib
import time
from zipfile import ZipFile

import pandas as pd
import pooch
from tqdm import tqdm

import toolbox

# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info


def get_cli_arguments():
    """Argument parser.

    This function parses the name of input files.

    Returns
    -------
    argparse.Namespace
        Object containing arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        action="store",
        type=str,
        help="Input file with file list.",
        required=True,
    )
    parser.add_argument(
        "--storage",
        action="store",
        type=str,
        help="Output directory to download files.",
        required=True,
    )
    parser.add_argument(
        "--type",
        action="extend",
        type=str,
        nargs="+",
        help="File extensions to download.",
        required=True,
    )
    parser.add_argument(
        "--withzipfiles",
        action="store_true",
        help="Include files within zip files.",
        default=False,
    )
    return parser.parse_args()


def select_files_to_download(filename, file_types, zipfiles="no"):
    """Load and merge datasets and files.

    Parameters
    ----------
    filename : str
        Name of file that contains file list
    file_types : list
        File extensions to download
    zipfiles : str
        Manage zip files.
        "no": list files of interest not in zip files.
        "yes": list files of interest inside zip files.
        "zip": list zip files that contain files of interest.
        Default: no

    Returns
    -------
    Pandas dataframe
        Select files dataframe
    """
    files_df = toolbox.load_database(filename, "files")
    print(f"Found {len(files_df)} files in {filename}")

    selected_files_df = pd.DataFrame()
    # List files of interest not in zip files.
    if zipfiles == "no":
        selected_files_df = files_df.query("from_zip_file == False").query(
            f"file_type in {file_types}"
        )
        print(
            f"Selected {len(selected_files_df)} files to download (NOT FROM zip files)"
        )
    # List files of interest inside zip files.
    if zipfiles == "yes":
        selected_files_df = files_df.query("from_zip_file == True").query(
            f"file_type in {file_types}"
        )
    # List zip files that contain files of interest.
    if zipfiles == "zip":
        selected_zip_df = (
            files_df.query("from_zip_file == True")
            .query(f"file_type in {file_types}")
            .loc[:, ["dataset_id", "origin_zip_file"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        print(
            f"Found {len(selected_zip_df)} zip files with intesting content to download"
        )
        selected_files_df = pd.merge(
            files_df,
            selected_zip_df,
            how="inner",
            left_on=["dataset_id", "file_name"],
            right_on=["dataset_id", "origin_zip_file"],
            suffixes=("", "_remove"),
        )
        # Remove duplicate columns.
        selected_files_df = selected_files_df.drop(
            [col for col in selected_files_df.columns if col.endswith("_remove")],
            axis="columns",
        )
        print(files_df.columns)
        print(selected_zip_df.columns)
        print(selected_files_df.columns)
        print(f"Selected {len(selected_files_df)} files to download (INSIDE zip files)")
    return selected_files_df


def download_file(
    url="", hash="", file_name="", path="", retry_if_failed=3, time_between_attempt=3
):
    """Download file.

    Parameters
    ----------
    url : str
        URL of file to download.
    hash : str
        MD5 hash.
    file_name : st
        Name of file.
    path : pathlib.Path
        Local path where file is stored.
    retry_if_failed : int
        Number of time to retry download if download fails.
    time_between_attempt : int
        Number of seconds to wait between download attempt.

    Returns
    -------
    pathlib.Path
        Absolute path of the downloaded file.
    """
    file_path = ""
    for attempt in range(retry_if_failed):
        try:
            file_path = pooch.retrieve(
                url=url,
                known_hash=f"md5:{hash}",
                fname=file_name,
                path=path,
                progressbar=True,
            )
        except Exception as exc:
            print(f"Cannot download {url} (attempt {attempt+1}/{retry_if_failed})")
            print(f"Will retry in {time_between_attempt} secondes")
            print(f"Exception type: {exc.__class__}")
            print(f"Exception message: {exc}\n")
            time.sleep(time_between_attempt)
        else:
            print(f"Downloaded: {path}/{file_name}")
            print(f"File size: {pathlib.Path(file_path).stat().st_size:,} bytes")
            break
    return pathlib.Path(file_path)


def extract_zip_content(files_lst, file_path):
    """Extract selected files from zip archive.

    Parameters
    ----------
    files_list : list of str
        Liste of files to extract from zip archive.
    file_path : pathlib.Path
        Absolute path of the zip file.
    """
    try:
        with ZipFile(file_path, "r") as zip_file:
            for file_name in files_lst:
                print(f"Extracting: {file_name}")
                zip_file.extract(file_name, path=file_path.parent)
    except Exception as exc:
        print(f"Cannot open zip archive: {file_path}")
        print(f"Exception type: {exc.__class__}")
        print(f"Exception message: {exc}\n")


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Create logger
    log_file = logging.FileHandler(
        f"{ARGS.input.replace('.tsv', '_download.log')}", mode="w"
    )
    log_file.setLevel(logging.INFO)
    log_stream = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_file, log_stream],
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    # Rewire the print function to logging.info
    print = logging.info

    # Print script name and doctring
    print(__file__)
    print(__doc__)

    # Verify input files exist
    toolbox.verify_file_exists(ARGS.input)

    # Create output dir
    pathlib.Path(ARGS.storage).mkdir(parents=True, exist_ok=True)

    print("File types to download:")
    for file_type in ARGS.type:
        print(f"- {file_type}")

    # Select files
    target_files_df = select_files_to_download(ARGS.input, ARGS.type)
    # Download files
    pbar = tqdm(
        target_files_df.index,
        leave=True,
        bar_format="--- {l_bar}{n_fmt}/{total_fmt} --- ",
    )
    for idx in pbar:
        dataset_origin = target_files_df.loc[idx, "dataset_origin"]
        dataset_id = target_files_df.loc[idx, "dataset_id"]
        repo_name = target_files_df.loc[idx, "dataset_origin"]
        file_path = download_file(
            url=target_files_df.loc[idx, "file_url"],
            hash=target_files_df.loc[idx, "file_md5"],
            file_name=target_files_df.loc[idx, "file_name"],
            path=pathlib.Path(ARGS.storage) / dataset_origin / dataset_id,
        )

    # If includezipfiles option is triggered
    if ARGS.withzipfiles:
        target_files_df = select_files_to_download(
            ARGS.input, ARGS.type, zipfiles="yes"
        )
        target_zip_df = select_files_to_download(ARGS.input, ARGS.type, zipfiles="zip")
        pbar = tqdm(
            target_zip_df.index,
            leave=True,
            bar_format="--- {l_bar}{n_fmt}/{total_fmt} --- ",
        )
        for idx in pbar:
            # Download zip file
            dataset_origin = target_zip_df.loc[idx, "dataset_origin"]
            dataset_id = target_zip_df.loc[idx, "dataset_id"]
            file_name = target_zip_df.loc[idx, "file_name"]
            file_path = download_file(
                url=target_zip_df.loc[idx, "file_url"],
                hash=target_zip_df.loc[idx, "file_md5"],
                file_name=file_name,
                path=pathlib.Path(ARGS.storage) / dataset_origin / dataset_id,
            )
            tmp_target_files = (
                target_files_df.query("dataset_origin == @dataset_origin")
                .query("dataset_id == @dataset_id")
                .query("origin_zip_file == @file_name")
                .loc[:, "file_name"]
                .tolist()
            )
            # Extract zip content
            extract_zip_content(tmp_target_files, file_path)

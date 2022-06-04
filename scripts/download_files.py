"""Download files.

We use Pooch, a Python library dedicated to fetch data files
https://www.fatiando.org/pooch/latest/index.html

Pooch can download a file and verify its integrity with a given hash.
It also uses a local cache and downloads data once.
"""

import argparse
import pathlib
import time
from zipfile import ZipFile

import pandas as pd
import pooch
from tqdm import tqdm


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
        "-i",
        "--input",
        action="store",
        type=str,
        help="Input file with file list.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        action="store",
        type=str,
        help="Output directory to download files.",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--type",
        action="append",
        type=str,
        help="File extensions to to download.",
        required=True,
    )
    parser.add_argument(
        "-z",
        "--includezipfiles",
        action="store_true",
        help="Include files within zip files.",
        default=False
    )
    return parser.parse_args()


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


def select_files_to_download(filename, file_types, withzipfiles=False):
    """Load and merge datasets and files.

    Parameters
    ----------
    filename : str
        Name of file that contains file list
    file_types : list
        File extensions to download
    withzipfiles : boolean
        Include zip files.
        Default: False

    Returns
    -------
    str
        Data repository name
    Pandas dataframe
        Select files dataframe
    """
    files_df = pd.read_csv(filename, sep="\t")
    print(f"Found {files_df.shape[0]} files in {filename}")
    repository_name = files_df.iloc[0]["dataset_origin"]
    print(f"Data repository: {repository_name}")

    selected_files_df = files_df.query("from_zip_file == False").query(
        f"file_type in {file_types}"
    )
    if withzipfiles:
        selected_zip_df = (files_df
            .query("from_zip_file == True")
            .query(f"file_type in {file_types}")
            .loc[:, ["dataset_id", "origin_zip_file"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        selected_files_df = pd.merge(
            files_df, 
            selected_zip_df, 
            how="inner", 
            left_on=["dataset_id", "file_name"], 
            right_on=["dataset_id", "origin_zip_file"]
        )
    print(f"Select {selected_files_df.shape[0]} files")
    return repository_name, selected_files_df


def download_file(url="", hash="", file_name="", path="", retry_if_failed=3, time_between_attempt=3):
    """Download file.

    Parameters
    ----------
    url : str
        URL of file to download.
    hash : str
        MD5 hash.
    file_name : st
        Name of file.
    path : str
        Local path where file is stored.
    retry_if_failed : int
        Number of time to retry download if download fails.
    time_between_attempt : int
        Number of seconds to wait between download attempt.
    
    Returns
    -------
    pathlib.Path
        Full path of downloaded file.
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
            print(f"Will retry in {time_between_attempt} s")
            print(f"Exception type: {exc.__class__}")
            print(f"Exception message: {exc}\n")
            time.sleep(time_between_attempt)
        else:
            break
    return pathlib.Path(file_path)


def extract_zip_content(file_path, selected_types):
    """Extract selected files from zip archive.

    Parameters
    ----------
    file_path : pathlib.Path
        Path of zip file.
    selected_types : list of str
        List of selected file types.
    """
    try:
        with ZipFile(file_path, "r") as zip_file:
            for file_name in zip_file.namelist():
                if file_name.startswith("__MACOSX"):
                    continue
                for file_type in selected_types:
                    if file_name.endswith(file_type):
                        print(f"Extracting {file_name} from {file_path}")
                        zip_file.extract(file_name, path=file_path.parent)
    except Exception as exc:
        print(f"Cannot open {file_path}")
        print(f"Exception type: {exc.__class__}")
        print(f"Exception message: {exc}\n")


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Verify input files exist
    verify_file_exists(ARGS.input)

    # Create output dir
    pathlib.Path(ARGS.output).mkdir(parents=True, exist_ok=True)

    print("File types to download:")
    for file_type in ARGS.type:
        print(f"- {file_type}")

    # Select files
    data_repo_name, target_df = select_files_to_download(ARGS.input, ARGS.type)

    # Download files
    pbar = tqdm(
        target_df.index,
        leave=True,
        bar_format="--- {l_bar}{n_fmt}/{total_fmt} --- ",
    )
    for idx in pbar:
        dataset_id = target_df.loc[idx, "dataset_id"]
        file_path = download_file(
            url=target_df.loc[idx, "file_url"], 
            hash=target_df.loc[idx, "file_md5"], 
            file_name=target_df.loc[idx, "file_name"], 
            path=f"{ARGS.output}/{data_repo_name}/{dataset_id}"
        )

    # If includezipfiles option is triggered
    if ARGS.includezipfiles:
        data_repo_name, target_df = select_files_to_download(ARGS.input, ARGS.type, withzipfiles=True)
        pbar = tqdm(
            target_df.index,
            leave=True,
            bar_format="--- {l_bar}{n_fmt}/{total_fmt} --- ",
        )
        for idx in pbar:
            # Download zip file
            dataset_id = target_df.loc[idx, "dataset_id"]
            file_path = download_file(
                url=target_df.loc[idx, "file_url"], 
                hash=target_df.loc[idx, "file_md5"], 
                file_name=target_df.loc[idx, "file_name"], 
                path=f"{ARGS.output}/{data_repo_name}/{dataset_id}"
            )
            # Extract zip content
            extract_zip_content(file_path, ARGS.type)

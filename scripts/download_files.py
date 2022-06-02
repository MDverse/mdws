"""Download files.

We use Pooch, a Python library dedicated to fetch data files
https://www.fatiando.org/pooch/latest/index.html

Pooch can download a file and verify its integrity with a given hash.
It also uses a local cache and downloads data once.
"""

import argparse
import pathlib
from zipfile import ZipFile

import pandas as pd
import pooch


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
    for idx in target_df.index:
        dataset_id = target_df.loc[idx, "dataset_id"]
        file_path = pooch.retrieve(
            url=target_df.loc[idx, "file_url"],
            known_hash=f"md5:{target_df.loc[idx, 'file_md5']}",
            fname=target_df.loc[idx, "file_name"],
            path=f"{ARGS.output}/{data_repo_name}/{dataset_id}",
            progressbar=True,
        )

    # If includezipfiles option is triggered
    if ARGS.includezipfiles:
        data_repo_name, target_df = select_files_to_download(ARGS.input, ARGS.type, withzipfiles=True)

        for idx in target_df.index:
            # Download zip files
            dataset_id = target_df.loc[idx, "dataset_id"]
            file_path = pooch.retrieve(
                url=target_df.loc[idx, "file_url"],
                known_hash=f"md5:{target_df.loc[idx, 'file_md5']}",
                fname=target_df.loc[idx, "file_name"],
                path=f"{ARGS.output}/{data_repo_name}/{dataset_id}",
                progressbar=True,
            )
            # Extract zip content
            file_path = pathlib.Path(file_path)
            with ZipFile(file_path, "r") as zip_file:
                # Get a list of all archived file names from the zip
                zip_file_list = zip_file.namelist()
                for filename in zip_file_list:
                    if filename.startswith("__MACOSX"):
                        continue
                    if ".DS_Store" in filename:
                        continue
                    print(f"Extracting {filename} from {file_path}")
                    zip_file.extract(filename, path=file_path.parent)

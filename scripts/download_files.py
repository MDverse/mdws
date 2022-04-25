"""Download files.

We use Pooch, a Python library dedicated to fetch data files
https://www.fatiando.org/pooch/latest/index.html

Pooch can download a file and verify its integrity with a given hash.
It also uses a local cache and downloads data once.
"""

import argparse
import pathlib

import pandas as pd
import pooch


def get_cli_arguments():
    """Argument parser.

    This function parses the name of input files.

    Returns
    -------
    str
        Name of the yaml input file.
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


def select_files_to_download(filename, file_types):
    """Load and merge datasets and files.

    Parameters
    ----------
    filename : str
        Name of file that contains file list
    file_types : list
        File extensions to download

    Returns
    -------
    str
        Data repository name
    Pandas dataframe
        Select files dataframe
    """
    files = pd.read_csv(filename, sep="\t")
    print(f"Found {files.shape[0]} files in {filename}")
    repository_name = files.iloc[0]["origin"]
    print(f"Data repository: {repository_name}")

    selected_files = files.query("from_zip_file == False").query(
        f"file_type in {file_types}"
    )
    print(f"Select {selected_files.shape[0]} files")
    return repository_name, selected_files


if __name__ == "__main__":
    args = get_cli_arguments()

    # Verify input files exist
    verify_file_exists(args.input)

    # Create output dir
    pathlib.Path(args.output).mkdir(parents=True, exist_ok=True)

    print("File types to download:")
    for file_type in args.type:
        print(file_type)

    # Select files
    data_repo_name, target_df = select_files_to_download(args.input, args.type)

    # Download files
    for idx in target_df.index:
        file_path = pooch.retrieve(
            url=target_df.loc[idx, "file_url"],
            known_hash=f"md5:{target_df.loc[idx, 'file_md5']}",
            fname=target_df.loc[idx, "file_name"],
            path=f"{args.output}/{data_repo_name}/{target_df.loc[idx, 'dataset_id']}",
            progressbar=True,
        )

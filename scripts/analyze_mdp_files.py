"""Analyze Gromacs mdp files.
"""

import argparse
from distutils import extension
import pathlib

import pandas as pd


FILE_TYPE = "mdp"


def get_cli_arguments():
    """Argument parser.

    This function parses command line arguments.

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
        help="Path to mdp files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        action="store",
        type=str,
        help="Output file",
        required=True,
    )
    return parser.parse_args()


def find_all_files(path, extension):
    """Find recursively all files with a given extension within a path.

    Parameters
    ----------
    path : str
        Path to find files
    extension : str
        Target file extension
    
    Returns
    -------
    List of pathlib.Path
        List of files.
    """
    files = list(pathlib.Path(path).glob(f"**/*.{extension}"))
    return files

if __name__ == "__main__":
    args = get_cli_arguments()
    print(args)

    target_files = find_all_files(args.input, FILE_TYPE)
    print(f"Found {len(target_files)} {FILE_TYPE} files in {args.input}")

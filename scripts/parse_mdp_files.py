"""Parse Gromacs mdp files.

Gromacs documentation on mdp file:
https://manual.gromacs.org/5.1.1/user-guide/mdp-options.html
mdp file example:
https://manual.gromacs.org/5.1.1/user-guide/file-formats.html#mdp
"""

import argparse
import pathlib
import re

import pandas as pd


REGEX_DT = re.compile("^\s*dt\s*=\s*([.\d]+)", re.IGNORECASE)
REGEX_NSTEPS = re.compile("^\s*nsteps\s*=\s*([\d]+)", re.IGNORECASE)
REGEX_TEMP = re.compile("^\s*(ref-t|ref_t)\s*=\s*([.\d]+)", re.IGNORECASE)
REGEX_THERMOSTAT = re.compile("^\s*tcoupl\s*=\s*([-\w]+)", re.IGNORECASE)
REGEX_BAROSTAT = re.compile("^\s*pcoupl\s*=\s*([-\w]+)", re.IGNORECASE)
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
        help="Path to find mdp files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        action="store",
        type=str,
        help="Path to save results",
        required=True,
    )
    return parser.parse_args()


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


def find_all_files(path, file_type):
    """Find recursively all files with a given extension within a path.

    Parameters
    ----------
    path : str
        Path to find files
    file_type : str
        Target file extension

    Returns
    -------
    List of pathlib.Path
        List of files.
    """
    files = list(pathlib.Path(path).glob(f"**/*.{file_type}"))
    return files


def extract_info_from_mdp(mdp_file_path, target_path):
    """Extract information from Gromacs mdp file.

    Parameters
    ----------
    mdp_file_path : str
        Path to mdp file
    target_path : str
        Path to the directory to find mdp files

    Returns
    -------
    dict
        Dictionnary of extracted informations
    """
    info = {
        "dataset_origin": None,
        "dataset_id": None,
        "dt": None,
        "nsteps": None,
        "temperature": None,
        "barostat": None,
        "thermostat": None,
        "filename": None,
    }
    info["dataset_origin"], info["dataset_id"], info["filename"] = str(
        mdp_file_path.relative_to(target_path)
    ).split("/", maxsplit=2)
    #print(f"Reading {str(mdp_file_path)}")
    with open(mdp_file_path, "r") as mdp_file:
        for line in mdp_file:
            # dt
            catch_dt = REGEX_DT.search(line)
            if catch_dt:
                info["dt"] = float(catch_dt.group(1))
            # nsteps
            catch_nsteps = REGEX_NSTEPS.search(line)
            if catch_nsteps:
                info["nsteps"] = int(catch_nsteps.group(1))
            catch_temp = REGEX_TEMP.search(line)
            # temperature
            if catch_temp:
                info["temperature"] = float(catch_temp.group(2))
            # tcoupl
            catch_thermostat = REGEX_THERMOSTAT.search(line)
            if catch_thermostat:
                info["thermostat"] = catch_thermostat.group(1)
            # pcoupl
            catch_barostat = REGEX_BAROSTAT.search(line)
            if catch_barostat:
                info["barostat"] = catch_barostat.group(1)
    return info


if __name__ == "__main__":
    args = get_cli_arguments()
    verify_output_directory(args.output)

    mdp_files = find_all_files(args.input, FILE_TYPE)
    print(f"Found {len(mdp_files)} {FILE_TYPE} files in {args.input}")

    mdp_info_lst = []
    for mdp_file in mdp_files:
        mdp_info = extract_info_from_mdp(mdp_file, args.input)
        mdp_info_lst.append(mdp_info)
    mdp_info_df = pd.DataFrame(mdp_info_lst)
    result_file_path = pathlib.Path(args.output) / "gromacs_mdp_files_info.tsv"
    mdp_info_df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Saved results in {str(result_file_path)}")
    print(f"Total number of mdp files parsed: {mdp_info_df.shape[0]}")

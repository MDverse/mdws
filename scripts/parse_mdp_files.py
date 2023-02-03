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
import numpy as np
from tqdm import tqdm

import toolbox

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
        "--input",
        action="extend",
        type=str,
        nargs="+",
        help="Path to find gro files.",
        required=True,
    )
    parser.add_argument(
        "--storage",
        action="store",
        type=str,
        help="Path in which gro files are stored.",
        required=True,
    )
    parser.add_argument(
        "--output",
        action="store",
        type=str,
        help="Path to save results",
        required=True,
    )
    return parser.parse_args()


def extract_info_from_mdp(mdp_file_path):
    """Extract information from Gromacs mdp file.

    Parameters
    ----------
    mdp_file_path : str
        Path to mdp file

    Returns
    -------
    dict
        Dictionnary of extracted informations
    """
    info = {
        "dt": np.nan,
        "nsteps": np.nan,
        "temperature": np.nan,
        "thermostat": None,
        "barostat": None,
        "is_error": False,
    }
    try:
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
                # thermostat
                catch_thermostat = REGEX_THERMOSTAT.search(line)
                if catch_thermostat:
                    info["thermostat"] = catch_thermostat.group(1)
                # barostat
                catch_barostat = REGEX_BAROSTAT.search(line)
                if catch_barostat:
                    info["barostat"] = catch_barostat.group(1)
    except (FileNotFoundError, UnicodeDecodeError, EOFError, OSError):
        print(f"\nCannot read: {mdp_file_path}")
        info["is_error"] = True
    return info


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Check input files
    for filename in ARGS.input:
        toolbox.verify_file_exists(filename)
    # check files path
    if not pathlib.Path(ARGS.storage).exists():
        raise FileNotFoundError(f"Directory {ARGS.storage} not found.")
    else:
        print(f"Found {ARGS.storage} folder.")
    # Check output directory
    toolbox.verify_output_directory(ARGS.output)

    # Create a dataframe with all files found in data repositories.
    df = pd.DataFrame()
    for filename in ARGS.input:
        files = pd.read_csv(
            filename,
            sep="\t",
            dtype={
                "dataset_id": str,
                "file_type": str,
                "file_md5": str,
                "file_url": str,
            },
        )
        df = pd.concat([df, files], ignore_index=True)

    df = df.query("file_type == 'mdp'")
    df["dt"] = np.nan
    df["nsteps"] = np.nan
    df["temperature"] = np.nan
    df["thermostat"] = None
    df["barostat"] = None
    print(f"Found {len(df)} files in inputs.")

    parsing_error_index = []
    pbar = tqdm(
        df.iterrows(),
        total=len(df),
        leave=True,
        bar_format="{l_bar}{n_fmt}/{total_fmt} [{elapsed}<{remaining}]{postfix}",
    )
    for index, row in pbar:
        mdp_file_name = (
            pathlib.Path(ARGS.storage)
            / row["dataset_origin"]
            / row["dataset_id"]
            / row["file_name"]
        )
        pbar.set_postfix({"file": str(mdp_file_name)})
        mdp_info = extract_info_from_mdp(mdp_file_name)
        # Keep track of files with error.
        if mdp_info["is_error"]:
            parsing_error_index.append(index)
        del mdp_info["is_error"]
        # Update dataframe with gro file info.
        for key in mdp_info:
            df.at[index, key] = mdp_info[key]

    # Remove files with parsing error.
    df = df.drop(index=parsing_error_index)

    # Remove unecessary columns.
    df = df.drop(
        columns=[
            "file_type",
            "file_size",
            "file_md5",
            "from_zip_file",
            "origin_zip_file",
            "file_url",
        ]
    )

    # Export results.
    result_file_path = pathlib.Path(ARGS.output) / "gromacs_mdp_files_info.tsv"
    df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Results saved in {str(result_file_path)}")
    print(f"Total number of mdp files parsed: {len(df)}")
    print(
        f"Number of mdp files skipped due to parsing error: "
        f"{len(parsing_error_index)}"
    )

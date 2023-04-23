"""Parse Gromacs mdp files.

Gromacs documentation on mdp file:
https://manual.gromacs.org/current/user-guide/mdp-options.html
mdp file example:
https://manual.gromacs.org/current/reference-manual/file-formats.html#mdp
"""

import argparse
import logging
import pathlib
import re
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm

import toolbox

# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info

# Regular expressions to extract information from mdp file.
# \s matchs any whitespace character (newline, tab, space, etc.)
# \w matchs any alphanumeric character (a-z, A-Z, 0-9, _)
REGEX_DT = re.compile("^\s*dt\s*=\s*([.\d]+)", re.IGNORECASE)
REGEX_NSTEPS = re.compile("^\s*nsteps\s*=\s*([\d]+)", re.IGNORECASE)
REGEX_TEMP = re.compile("^\s*(ref-t|ref_t)\s*=\s*([.\d]+)", re.IGNORECASE)
REGEX_THERMOSTAT = re.compile("^\s*tcoupl\s*=\s*([-\w]+)", re.IGNORECASE)
REGEX_BAROSTAT = re.compile("^\s*pcoupl\s*=\s*([-\w]+)", re.IGNORECASE)
REGEX_INTEGRATOR = re.compile("^\s*integrator\s*=\s*([-\w]+)", re.IGNORECASE)
FILE_TYPE = "mdp"

# Normalized thermostat and barostat names
# https://manual.gromacs.org/documentation/current/user-guide/mdp-options.html#temperature-coupling
THERMOSTATS = {
    "no": "no",
    "berendsen": "Berendsen",
    "nosehoover": "Nose-Hoover",
    "andersen": "Andersen",
    "andersenmassive": "Andersen-massive",
    "vrescale": "V-rescale",
}
# https://manual.gromacs.org/documentation/current/user-guide/mdp-options.html#pressure-coupling
BAROSTATS = {
    "no": "no",
    "berendsen": "Berendsen",
    "crescale": "C-rescale",
    "parrinellorahman": "Parrinello-Rahman",
    "mttk": "MTTK",
}


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
        "integrator": None,
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
                # integrator
                catch_integrator = REGEX_INTEGRATOR.search(line)
                if catch_integrator:
                    info["integrator"] = catch_integrator.group(1)
    except (FileNotFoundError, UnicodeDecodeError, EOFError, OSError):
        print(f"Cannot read: {mdp_file_path}")
        info["is_error"] = True
    return info


def normalize_thermostat_barostat(
    dataframe=pd.DataFrame(),
    column_to_normalize="thermostat",
    value_undefined="undefined",
    value_unknown="unknown"
    ):
    """Normalize thermostat and barostat parameter.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Pandas dataframe with data to normalize.
    column_to_normalize : str
        Column to normalize in the dataframe.
        Either 'thermostat' or 'barostat'.
    value_undefined : str, optional
        Value to return if value is not a string, by default "undefined".
    value_unknown : str, optional
        Value to return if value is not in normalized_values, by default "unknown".

    Returns
    -------
    pd.DataFrame
        Pandas dataframe with normalized thermostat or barostat.
    """
    if column_to_normalize not in ["thermostat", "barostat"]:
        print("Column value should be 'thermostat' or 'barostat'")
        sys.exit(1)
    print(f"Normalizing {column_to_normalize} values...")
    REFERENCE = dict()
    if column_to_normalize == "thermostat":
        REFERENCE = THERMOSTATS
    else:
        REFERENCE = BAROSTATS
    for index, row in dataframe.iterrows():
        normalized_value = ""
        value = row[column_to_normalize]
        if type(value) is not str:
            normalized_value = value_undefined
        else:
            value_tmp = value.lower().replace("-", "").replace("_", "")
            normalized_value = REFERENCE.get(value_tmp, value_unknown)
        df.at[index, column_to_normalize] = normalized_value
        if normalized_value == value_unknown:
            print(f"In: {row['dataset_origin']} / {row['dataset_id']}")
            print(f"file: {row['file_name']}")
            print(f"has an unknown {column_to_normalize} value: {value}")
            print(f"Value set to: '{value_unknown}'")
    return df


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Create logger.
    log_file = logging.FileHandler(
        pathlib.Path(ARGS.output) / "gromacs_mdp_files.log", mode="w"
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

    # Print script name and the first line of the doctring.
    print(__file__)
    print(__doc__.split("\n")[0])

    # Check input files.
    for filename in ARGS.input:
        toolbox.verify_file_exists(filename)
    # check files path
    if not pathlib.Path(ARGS.storage).exists():
        raise FileNotFoundError(f"Directory {ARGS.storage} not found.")
    else:
        print(f"Found {ARGS.storage} folder.")
    # Check output directory.
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
    df["integrator"] = None
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

    # Normalize thermostat values.
    df = normalize_thermostat_barostat(
        dataframe=df,
        column_to_normalize="thermostat",
    )

    # Normalize barostat values.
    df = normalize_thermostat_barostat(
        dataframe=df,
        column_to_normalize="barostat",
    )

    # Export results.
    result_file_path = pathlib.Path(ARGS.output) / "gromacs_mdp_files.tsv"
    df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Results saved in {str(result_file_path)}")
    print(f"Number of mdp files parsed: {len(df)}")
    print(
        f"Number of mdp files skipped due to parsing error: "
        f"{len(parsing_error_index)}"
    )

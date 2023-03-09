"""Parse Gromacs gro files.

Gromacs documentation on gro file:
https://manual.gromacs.org/5.1.1/user-guide/file-formats.html#gro
"""

import argparse
import itertools
import logging
import pathlib
import warnings


import MDAnalysis as mda
import pandas as pd
import numpy as np
import tqdm
import yaml


import toolbox

# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info

# Ignore warnings that have no consequence here:
# cannot guess atom mass and missing velocities.
warnings.filterwarnings(
    "ignore",
    message="Failed to guess the mass for the following atom types:",
    category=UserWarning,
    module="MDAnalysis",
)

warnings.filterwarnings(
    "ignore",
    message="Not all velocities were present.",
    category=UserWarning,
    module="MDAnalysis",
)


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
        "--residues",
        action="store",
        type=str,
        help="Yaml file with residue definition.",
        required=True,
    )
    parser.add_argument(
        "--output",
        action="store",
        type=str,
        help="Path to save results.",
        required=True,
    )
    return parser.parse_args()


def read_residue_file(residue_filename):
    """Read residue definition file.

    Parameters
    ----------
    residue_filename : str
        Name of file containing residue definition

    Returns
    -------
    dict
        Dictionnary with set of residues by type (protein, lipid...)
    """
    with open(residue_filename, "r") as residue_file:
        print(f"Reading residue definition from: {residue_filename}")
        data_loaded = yaml.safe_load(residue_file)
    residues = {
        "protein": set(data_loaded["protein"]),
        "lipid": set(data_loaded["lipid"]),
        "nucleic": set(data_loaded["nucleic"]),
        "water_ion": set(data_loaded["water_ion"]),
        "glucid": set(data_loaded["glucid"]),
    }
    # Print number of residues for each type.
    for residue_type in residues:
        print(f"Found {len(residues[residue_type])} residues for '{residue_type}'")
    # Remove duplicates between residue types.
    for residue_type_1, residue_type_2 in itertools.combinations(residues, 2):
        common_residues = residues[residue_type_1] & residues[residue_type_2]
        for res in common_residues:
            print(f"residue {res}: found in both '{residue_type_1}' and '{residue_type_2}'")
            print(f"residue {res}: removed from list '{residue_type_1}'")
            residues[residue_type_1].remove(res)
            print(f"residue {res}: removed from list '{residue_type_2}'")
            residues[residue_type_2].remove(res)
    return residues


def extract_info_from_gro(gro_file_path="", residues={}):
    """Extract information from Gromacs mdp file.

    Parameters
    ----------
    gro_file_path : str
        Path to gro file
    residues : dict
        Dictionnary with set of residues by type (protein, lipid...)

    Returns
    -------
    dict
        Dictionnary of extracted informations
    """
    info = {
        "atom_number": None,
        "has_protein": False,
        "has_nucleic": False,
        "has_lipid": False,
        "has_water_ion": False,
        "has_glucid": False,
        "is_error": False,
    }
    try:
        universe = mda.Universe(gro_file_path)
        info["atom_number"] = len(universe.atoms)
        residue_names = set(universe.residues.resnames)
        for residue_name in residue_names:
            residue_not_found = True
            for residue_type in residues:
                if residue_name in residues[residue_type]:
                    info[f"has_{residue_type}"] = True
                    residue_not_found = False
            if residue_not_found:
                # WALL particles
                if residue_name in ["WAL"]:
                    pass
                else:
                    print(f"In file: {str(gro_file_path)}")
                    print(f"Found unknown residue: {residue_name}")
    except (
        IndexError,
        ValueError,
        UnicodeDecodeError,
        EOFError,
        OSError,
        UnboundLocalError,
    ):
        print(f"\nCannot read: {gro_file_path}")
        info["is_error"] = True
    return info


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Create logger.
    log_file = logging.FileHandler(
        pathlib.Path(ARGS.output) / "gromacs_gro_files.log", mode="w"
    )
    log_file.setLevel(logging.INFO)
    log_stream = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_file, log_stream],
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    # Rewire the print function to logging.info
    print = logging.info

    # Print script name and the first line of the doctring.
    print(__file__)
    print(__doc__.split("\n")[0])

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

    # Read residue definition file.
    RESIDUES_DICT = read_residue_file(ARGS.residues)

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

    df = df.query("file_type == 'gro'")
    df["atom_number"] = np.nan
    df["has_protein"] = False
    df["has_nucleic"] = False
    df["has_lipid"] = False
    df["has_glucid"] = False
    df["has_water_ion"] = False

    print(f"Found {len(df)} gro files in inputs.")

    gro_info_lst = []
    parsing_error_index = []
    pbar = tqdm.tqdm(
        df.iterrows(),
        total=len(df),
        leave=True,
        bar_format="{l_bar}{n_fmt}/{total_fmt} [{elapsed}<{remaining}]{postfix}",
    )
    for index, row in pbar:
        gro_file_name = (
            pathlib.Path(ARGS.storage)
            / row["dataset_origin"]
            / row["dataset_id"]
            / row["file_name"]
        )
        pbar.set_postfix({"file": str(gro_file_name)})
        # pbar.set_description(f"Reading {gro_file_name}", refresh=True)
        gro_info = extract_info_from_gro(
            gro_file_name,
            RESIDUES_DICT
        )
        # Keep track of files with error.
        if gro_info["is_error"]:
            parsing_error_index.append(index)
        del gro_info["is_error"]
        # Update dataframe with gro file info.
        for key in gro_info:
            df.at[index, key] = gro_info[key]

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
    result_file_path = pathlib.Path(ARGS.output) / "gromacs_gro_files.tsv"
    df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Results saved in {str(result_file_path)}")
    print(f"Total number of gro files parsed: {len(df)}")
    print(
        f"Number of gro files skipped due to parsing error: "
        f"{len(parsing_error_index)}"
    )

"""Parse Gromacs gro files.

Gromacs documentation on gro file:
https://manual.gromacs.org/5.1.1/user-guide/file-formats.html#gro
"""

import argparse
import pathlib
from unicodedata import unidata_version
import warnings


import MDAnalysis as mda
import pandas as pd
import numpy as np
import tqdm
import yaml


import toolbox


# Ignore warnings that have no consequence here: cannot guess atom mass and missing velocities
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
    protein_residues : list
        List of protein residues
    lipid_residues : list
        List of lipid residues
    nucleic_residues : lst
        List of nucleic acid residues
    """
    with open(residue_filename, "r") as residue_file:
        print(f"Reading residue definition from: {residue_filename}")
        data_loaded = yaml.safe_load(residue_file)
    protein_residues = data_loaded["protein"]
    lipid_residues = data_loaded["lipid"]
    nucleic_residues = data_loaded["nucleic"]
    water_ion_residues = data_loaded["water_ion"]
    glucid_residues = data_loaded["glucid"]
    return (
        protein_residues,
        lipid_residues,
        nucleic_residues,
        water_ion_residues,
        glucid_residues,
    )


def extract_info_from_gro(
    gro_file_path="",
    protein_residues=[],
    lipid_residues=[],
    nucleic_residues=[],
    water_ion_residues=[],
    glucid_residues=[],
):
    """Extract information from Gromacs mdp file.

    Parameters
    ----------
    gro_file_path : str
        Path to gro file
    protein_residues : list
        List of protein residues
    lipid_residues : list
        List of lipid residues
    nucleic_residues : lst
        List of nucleic acid residues
    water_ion_residues : lst
        List of water and ions
    glucid_residues : lst
        List of glucid residues

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
            if residue_name in protein_residues:
                info["has_protein"] = True
            elif residue_name in lipid_residues:
                info["has_lipid"] = True
            elif residue_name in nucleic_residues:
                info["has_nucleic"] = True
            elif residue_name in water_ion_residues:
                info["has_water_ion"] = True
            elif residue_name in glucid_residues:
                info["has_glucid"] = True
            # WALL particles
            elif residue_name in ["WAL"]:
                pass
            else:
                pass
                # print(f"Unknown residue: {residue_name} / {str(gro_file_path)}")
    except (ValueError, UnicodeDecodeError, EOFError, OSError, UnboundLocalError):
        print(f"\nCannot read: {gro_file_path}")
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

    # Read residue definition file.
    (
        PROTEIN_RESIDUES,
        LIPID_RESIDUES,
        NUCLEIC_RESIDUES,
        WATER_ION_RESIDUES,
        GLUCID_RESIDUES,
    ) = read_residue_file(ARGS.residues)

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
            PROTEIN_RESIDUES,
            LIPID_RESIDUES,
            NUCLEIC_RESIDUES,
            WATER_ION_RESIDUES,
            GLUCID_RESIDUES,
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
    result_file_path = pathlib.Path(ARGS.output) / "gromacs_gro_files_info.tsv"
    df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Results saved in {str(result_file_path)}")
    print(f"Total number of gro files parsed: {len(df)}")
    print(
        f"Number of gro files skipped due to parsing error: "
        f"{len(parsing_error_index)}"
    )

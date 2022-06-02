"""Parse Gromacs gro files.

Gromacs documentation on gro file:
https://manual.gromacs.org/5.1.1/user-guide/file-formats.html#gro
"""

import argparse
import pathlib

import pandas as pd
from tqdm import tqdm
import yaml

FILE_TYPE = "gro"


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
        help="Path to find gro files",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--residues",
        action="store",
        type=str,
        help="Yaml file with residue definition",
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
        print(f"Reading residue definition fom: {residue_filename}")
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
    file_lst = list(pathlib.Path(path).glob(f"**/*.{file_type}"))
    return file_lst


def extract_info_from_gro(
    gro_file_path="",
    target_path="",
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
    target_path : str
        Path to the directory to find gro files
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
        "dataset_origin": None,
        "dataset_id": None,
        "atom_number": None,
        "has_protein": False,
        "has_nucleic": False,
        "has_lipid": False,
        "has_water_ion": False,
        "has_glucid": False,
        "filename": None,
    }
    # Extract repository name, dataset id and file name from file path
    # For instance, extract:
    # dataset_origin: zenodo
    # dataset_id: 3862992
    # filename: mdia2-30-r1.gro
    # from
    # gro_file_path: data/downloads/zenodo/3862992/mdia2-30-r1.gro
    # target_path: data/downloads
    info["dataset_origin"], info["dataset_id"], info["filename"] = str(
        gro_file_path.relative_to(target_path)
    ).split("/", maxsplit=3)
    try:
        with open(gro_file_path, "r") as gro_file:
            for idx, line in enumerate(gro_file):
                # Some .gro files are sometimes badly formatted
                # so we need to be extra cautious
                try:
                    # The first line (idx=0) is a comment
                    # The second line (idx=1) is the number of atoms
                    if idx == 1:
                        info["atom_number"] = int(line)
                    if (
                        (idx > 1)
                        and (len(line.rstrip()) <= 70)
                        and (len(line.split()) > 3)
                    ):
                        residue_number = int(line[0:5])
                        residue_name = line[5:10].strip()
                        atom_name = line[10:15].strip()
                        atom_number = int(line[15:20])
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
                except:
                    print(f"\nCannot read {gro_file_path} at line {idx+1}")
                    print(f"Faulty line: {line.strip()}")
                    break
    except UnicodeDecodeError:
        print(f"\nCannot read: {gro_file_path}")
    return info


if __name__ == "__main__":
    args = get_cli_arguments()
    verify_output_directory(args.output)
    (
        PROTEIN_RESIDUES,
        LIPID_RESIDUES,
        NUCLEIC_RESIDUES,
        WATER_ION_RESIDUES,
        GLUCID_RESIDUES,
    ) = read_residue_file(args.residues)

    GRO_FILES_LST = find_all_files(args.input, FILE_TYPE)
    print(f"Found {len(GRO_FILES_LST)} {FILE_TYPE} files in {args.input}")
    GRO_FILE_NUMBER = len(GRO_FILES_LST)

    gro_info_lst = []
    pbar = tqdm(
        GRO_FILES_LST,
        leave=True,
        bar_format="{l_bar}{n_fmt}/{total_fmt} [{elapsed}<{remaining}]{postfix}",
    )
    for gro_file_name in pbar:
        pbar.set_postfix({"file": str(gro_file_name)})
        # pbar.set_description(f"Reading {gro_file_name}", refresh=True)
        gro_info = extract_info_from_gro(
            gro_file_name,
            args.input,
            PROTEIN_RESIDUES,
            LIPID_RESIDUES,
            NUCLEIC_RESIDUES,
            WATER_ION_RESIDUES,
            GLUCID_RESIDUES,
        )
        gro_info_lst.append(gro_info)
    gro_info_df = pd.DataFrame(gro_info_lst)
    result_file_path = pathlib.Path(args.output) / "gromacs_gro_files_info.tsv"
    gro_info_df.to_csv(result_file_path, sep="\t", index=False)
    print(f"Saved results in {str(result_file_path)}")
    print(f"Total number of gro files parsed: {gro_info_df.shape[0]}")

"""Remove datasets that contains only non-MD files.


python scripts/remove_false_positives.py --input data/ --repo zenodo --filetypes params/file_types.yml

"""

import argparse
import logging

import pandas as pd
import yaml


def get_cli_arguments():
    """Commande line argument parser.

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
        help="Path to find tsv files",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--repo",
        action="store",
        type=str,
        help="Name of repository: zenodo, figshare, osf",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--filetypes",
        action="store",
        type=str,
        help="Path to the MD file types definition file",
        required=True,
    )
    return parser.parse_args()


def read_md_files(filename):
    """Read MD files definition file.

    Parameters
    ----------
    filename : str
        Path to the MD files definition file.

    Returns
    -------
    list
        List of MD file types without zip.
    """
    with open(filename, "r") as filetypes_file:
        data_loaded = yaml.safe_load(filetypes_file)
    md_files = data_loaded["file_types"]
    md_types = [extension["type"] for extension in md_files]
    md_types.remove("zip")
    print(f"Found {len(md_types)} MD file types")
    return md_types


def find_false_positive_datasets(filename, md_file_types):
    """Find false positive datasets.

    False positive datasets are datasets that propably do not
    contain any molecular dynamics data.
    
    Parameters
    ----------
    filename : str
        Path to the file which contains all files from a given repo.
    md_file_types: list
        List containing molecular dynamics file types.

    Returns
    -------
    list
        Dictionary of false positive datasets
    """
    df = pd.read_csv(files_filename, sep="\t")
    df["file_type"] = df["file_type"].astype(str)
    unique_file_types_per_dataset = (df
        .groupby("dataset_id")["file_type"]
        .agg(["count", "unique"])
        .sort_values(by="count", ascending=False)
    )
    false_positives = []
    for index in unique_file_types_per_dataset.index:
        file_types = list(unique_file_types_per_dataset.loc[index, "unique"])
        number_files = unique_file_types_per_dataset.loc[index, "count"]
        # Datasets that only contain zip files might have not been properly
        # parsed by the scrapper or zip preview is not available.
        # In case of doubt, we keep these datasets.
        if file_types == ["zip"]:
            print(f"Dataset {index}: only zip files -> keep")
            continue
        # For a fiven dataset, if there is no MD file types in the entire set 
        # of the dataset file types, then we might have a false positive dataset.
        if len(set(file_types) & set(md_file_types)) == 0:
            print(f"Dataset {index} might be a false positive ({number_files} files)")
            print(" ".join(file_types[:20]))
            print("---")
            false_positives.append(index)
    return false_positives


def remove_false_positive_datasets(filename, dataset_ids_to_remove):
    """Remove false positive datasets from file.

    Parameters
    ----------
    filename : str
        Path to the data file
    dataset_ids_to_remove : list
        List of dataset ids to remove
    """
    df = pd.read_csv(filename, sep="\t")
    records_count_old = df.shape[0]
    df_clean = df[~df["dataset_id"].isin(dataset_ids_to_remove)]
    records_count_clean = df_clean.shape[0]
    filename_clean = filename.replace(".tsv", ".clean.tsv")
    print(f"Removing {records_count_old - records_count_clean} lines "
          f"({records_count_old} -> {records_count_clean}) : {filename_clean}")
    df_clean.to_csv(filename_clean, sep="\t", index=False)


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Create logger
    log_file = logging.FileHandler(f"{ARGS.input}/{ARGS.repo}_clean.log", mode="w")
    log_console = logging.StreamHandler()
    logging.basicConfig(handlers=[log_file, log_console],
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG
    )
    # Rewire print to logger
    print = logging.info

    md_file_types = read_md_files(ARGS.filetypes)

    datasets_filename = f"{ARGS.input}/{ARGS.repo}_datasets.tsv"
    texts_filename = f"{ARGS.input}/{ARGS.repo}_datasets_text.tsv"
    files_filename = f"{ARGS.input}/{ARGS.repo}_files.tsv"

    false_positive_datasets = find_false_positive_datasets(files_filename, md_file_types)

    remove_false_positive_datasets(files_filename, false_positive_datasets)
    remove_false_positive_datasets(datasets_filename, false_positive_datasets)
    remove_false_positive_datasets(texts_filename, false_positive_datasets)


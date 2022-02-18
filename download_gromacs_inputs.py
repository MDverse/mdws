"""Download Gromacs input files.

We use Pooch, a Python library dedicated to fetch data files
https://www.fatiando.org/pooch/latest/index.html

Pooch can download a file and verify its integrity with a given hash.
It also uses a local cache and downloads data once.
"""

import pandas as pd
import pooch


def load_datasets_and_files(datasets_name, files_name):
    """Load and merge datasets and files.

    Parameters
    ----------
    datasets_name : str
        Name of file that contains datasets
    files_name : str
        Name of file that contains files

    Returns
    -------
    Pandas dataframe
        Datasets dataframe
    Pandas dataframe
        Files dataframe
    Pandas dataframe
        Merged dataframe
    """
    datasets = pd.read_csv(datasets_name, sep="\t")
    print(f"Found {datasets.shape[0]} datasets.")
    files = pd.read_csv(files_name, sep="\t")
    print(f"Found {files.shape[0]} files.")
    tab = pd.merge(
        files, datasets, how="left", on=["dataset_id", "origin"], validate="many_to_one"
    )
    print(f"Dataframe has {tab.shape[0]} entries.")
    return datasets, files, tab


datasets_df, files_df, all_df = load_datasets_and_files(
    "zenodo_datasets.tsv", "zenodo_files.tsv"
)


target_df = all_df.query("from_zip_file == False").query("file_type in ['mdp', 'gro']")

print(f"Number of files to download: {target_df.shape[0]}")

for idx in target_df.index:
    file_path = pooch.retrieve(
        url=target_df.loc[idx, "file_url"],
        known_hash=f"md5:{target_df.loc[idx, 'file_md5']}",
        fname=target_df.loc[idx, "file_name"],
        path=f"zenodo_download/zenodo_{target_df.loc[idx, 'dataset_id']}",
        progressbar=True,
    )

"""Convert datasets to parquet format."""


import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def compute_global_statistics(df):
    """Compute global statistics.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe with merged datasets and files.
    """
    df["_is_zip_file"] = np.where((df["file_type"] == "zip"), True, False)
    df["_is_not_from_zip_file"] = ~df["from_zip_file"]
    df["_size_not_from_zip_file"] = np.where(
        (df["from_zip_file"] == False), df["file_size"] / 1e9, 0.0
    )
    dataset_agg = df.groupby("dataset_origin").agg(
        datasets=("dataset_id", "nunique"),
        files_from_datasets=("_is_not_from_zip_file", "sum"),
        total_size_in_GB=("_size_not_from_zip_file", "sum"),
        zip_files=("_is_zip_file", "sum"),
        files_from_zip_files=("from_zip_file", "sum"),
        total_files=("dataset_id", "count"),
    )
    dataset_agg.loc["total"] = dataset_agg.sum(numeric_only=True)
    pd.set_option("display.precision", 0)
    print(dataset_agg)


def compare_types(pandas_df, parquet_file):
    """Compare pandas and parquet types.

    Parameters
    ----------
    pandas_df : pd.DataFrame
        Pandas dataframe with data.
    parquet_file : str
        Name of parquet file.
    """
    parquet_schema = pq.read_schema(parquet_file, memory_map=True)
    parquet_types = pd.DataFrame(
        {"column_name": parquet_schema.names, "parquet_type": parquet_schema.types}
    )
    pandas_types = pd.DataFrame(
        {"column_name": pandas_df.dtypes.index, "pandas_type": pandas_df.dtypes.values}
    )
    merge_types = pd.merge(
        pandas_types,
        parquet_types,
        how="inner",
        on="column_name",
        validate="one_to_one",
    )
    print(merge_types)


if __name__ == "__main__":
    # Merge all datasets and convert to parquet
    datasets_df = pd.DataFrame()
    for repository in ["zenodo", "figshare", "osf"]:
        name = f"data/{repository}_datasets.tsv"
        tmp_df = pd.read_csv(name, sep="\t", dtype={"dataset_id": str})
        print(f"Found {len(tmp_df)} datasets in {name}.")
        name_text = f"data/{repository}_datasets_text.tsv"
        tmp_df_text = pd.read_csv(name_text, sep="\t", dtype={"dataset_id": str})
        print(f"Found {len(tmp_df_text)} datasets in {name_text}.")
        merged_df = pd.merge(
            tmp_df,
            tmp_df_text,
            how="left",
            on=["dataset_id", "dataset_origin"],
            validate="one_to_one",
        )
        datasets_df = pd.concat([datasets_df, merged_df], ignore_index=True)
    output_name = "data/datasets.parquet"
    datasets_df.to_parquet(output_name)
    print(f"Wrote {output_name}")

    # Merge all files and convert to parquet
    files_df = pd.DataFrame()
    for repository in ["zenodo", "figshare", "osf"]:
        name = f"data/{repository}_files.tsv"
        tmp_df = pd.read_csv(
            name,
            sep="\t",
            dtype={
                "dataset_id": str,
                "file_type": str,
                "file_md5": str,
                "file_url": str,
            },
        )
        print(f"Found {len(tmp_df)} files in {name}.")
        files_df = pd.concat([files_df, tmp_df], ignore_index=True)
    output_name = "data/files.parquet"
    files_df.to_parquet(output_name)
    print(f"Wrote {output_name}")

    # Verify column types are the same between Parquet and original TSV files.
    tsv_df = pd.merge(
        files_df,
        datasets_df,
        how="left",
        on=["dataset_id", "dataset_origin"],
        validate="many_to_one",
    )
    parquet_df = pd.merge(
        pd.read_parquet("data/files.parquet"),
        pd.read_parquet("data/datasets.parquet"),
        how="left",
        on=["dataset_id", "dataset_origin"],
        validate="many_to_one",
    )

    print("Statistics from TSV files:")
    compute_global_statistics(tsv_df)
    print()
    print("Statistics from Parquet files:")
    compute_global_statistics(parquet_df)

    # Convert Gromacs gro files info to parquet
    name = "data/gromacs_gro_files_info.tsv"
    df = pd.read_csv(
        name,
        sep="\t",
        dtype={
            "dataset_origin": str,
            "dataset_id": str,
            "file_name": str,
            "atom_number": int,
            "bas_protein": bool,
            "has_nucleic": bool,
            "has_lipid": bool,
            "has_glucid": bool,
            "has_water_ion": bool,
        },
    )
    print()
    print(f"Found {len(df)} files in {name}.")
    output_name = "data/gromacs_gro_files.parquet"
    df.to_parquet(output_name)
    print(f"Wrote {output_name}")
    compare_types(df, output_name)

    # Convert Gromacs mdp files info to parquet
    name = "data/gromacs_mdp_files_info.tsv"
    df = pd.read_csv(
        name,
        sep="\t",
        dtype={
            "dataset_origin": str,
            "dataset_id": str,
            "file_name": str,
            "dt": float,
            "nsteps": float,
            "temperature": float,
            "thermostat": str,
            "barostat": str,
        },
    )
    print()
    print(f"Found {len(df)} files in {name}.")
    output_name = "data/gromacs_mdp_files.parquet"
    df.to_parquet(output_name)
    print(f"Wrote {output_name}")
    compare_types(df, output_name)

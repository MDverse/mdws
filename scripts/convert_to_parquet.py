"""Convert datasets to parquet format."""

import pandas as pd


# Merge all datasets and convert to parquet
df = pd.DataFrame()
for repository in ["zenodo", "figshare", "osf"]:
    name = f"data/{repository}_datasets.tsv"
    tmp_df = pd.read_csv(
        name,
        sep="\t",
        dtype={"dataset_id": str}
    )
    print(f"Found {len(tmp_df)} datasets in {name}.")
    name_text = f"data/{repository}_datasets_text.tsv"
    tmp_df_text = pd.read_csv(
        name_text,
        sep="\t",
        dtype={"dataset_id": str}
    )
    print(f"Found {len(tmp_df_text)} datasets in {name_text}.")
    merged_df = pd.merge(tmp_df, tmp_df_text, how="left", on=["dataset_id", "dataset_origin"], validate="one_to_one")
    df = pd.concat([df, merged_df], ignore_index=True)
output_name = "data/datasets.parquet"
df.to_parquet(output_name)
print(f"Wrote {output_name}")


# Merge all files and convert to parquet
df = pd.DataFrame()
for repository in ["zenodo", "figshare", "osf"]:
    name = f"data/{repository}_files.tsv"
    tmp_df = pd.read_csv(
        name,
        sep="\t",
        dtype={"dataset_id": str, "file_type": str,
               "file_md5": str, "file_url": str}
    )
    print(f"Found {len(tmp_df)} files in {name}.")
    df = pd.concat([df, tmp_df], ignore_index=True)
output_name = "data/files.parquet"
df.to_parquet(output_name)
print(f"Wrote {output_name}")


# Convert Gromacs gro files info to parquet
name = "data/gromacs_gro_files_info.tsv"
df = pd.read_csv(
    name,
    sep="\t",
    dtype={"dataset_origin": str, "dataset_id": str, "file_name": str,
            "atom_number": int, "bas_protein": bool, "has_nucleic": bool, 
            "has_lipid": bool, "has_glucid": bool, "has_water_ion": bool}
)
print(f"Found {len(df)} files in {name}.")
output_name = "data/gromacs_gro_files.parquet"
df.to_parquet(output_name)
print(f"Wrote {output_name}")


# Convert Gromacs mdp files info to parquet
name = "data/gromacs_mdp_files_info.tsv"
df = pd.read_csv(
    name,
    sep="\t",
    dtype={"dataset_origin": str, "dataset_id": str, "file_name": str,
            "dt": float, "nsteps": float, "temperature": float, 
            "thermostat": str, "barostat": str}
)
print(f"Found {len(df)} files in {name}.")
print(df.dtypes)
output_name = "data/gromacs_mdp_files.parquet"
df.to_parquet(output_name)
print(f"Wrote {output_name}")
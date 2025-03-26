#! /usr/bin/env bash

echo "Updating Python virtual environment"

uv sync

echo "SCRAPING ZENODO"

uv run scripts/scrap_zenodo.py --query params/query.yml --output data

echo "SCRAPING FIGSHARE"

uv run scripts/scrap_figshare.py --query params/query.yml --output data

echo "SCRAPING OSF"

uv run scripts/scrap_osf.py --query params/query.yml --output data


echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM ZENODO"

uv run scripts/download_files.py --input data/zenodo_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles

echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM FIGSHARE"

uv run scripts/download_files.py --input data/figshare_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles

echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM OSF"

uv run scripts/download_files.py --input data/osf_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles


echo "PARSING GROMACS .MDP FILES"

uv run scripts/parse_mdp_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --output data


echo "PARSING GROMACS .GRO FILES"

uv run scripts/parse_gro_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --residues params/residue_names.yml --output data


echo "EXPORTING TO PARQUET"

uv run scripts/export_to_parquet.py


echo "ALL JOBS DONE!"

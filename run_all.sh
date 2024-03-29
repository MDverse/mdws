#! /usr/bin/env bash

if [[ ! ${CONDA_PREFIX} =~ ^.*mdws$ ]]
then
    echo "Please activate the mdws conda environment"
    exit 1
fi

echo "Conda environment is in: ${CONDA_PREFIX}"

echo "SCRAPING ZENODO"

python scripts/scrap_zenodo.py --query params/query.yml --output data

echo "SCRAPING FIGSHARE"

python scripts/scrap_figshare.py --query params/query.yml --output data

echo "SCRAPING OSF"

python scripts/scrap_osf.py --query params/query.yml --output data


echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM ZENODO"

python scripts/download_files.py --input data/zenodo_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles

echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM FIGSHARE"

python scripts/download_files.py --input data/figshare_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles

echo "DOWNLOADING GROMACS .MDP and .GRO FILES FROM OSF"

python scripts/download_files.py --input data/osf_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles


echo "PARSING GROMACS .MDP FILES"

python scripts/parse_mdp_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --output data


echo "PARSING GROMACS .GRO FILES"

python scripts/parse_gro_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --residues params/residue_names.yml --output data


echo "EXPORTING TO PARQUET"

python scripts/export_to_parquet.py


echo "ALL JOBS DONE!"

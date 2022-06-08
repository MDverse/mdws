# Molecular Dynamics web scrapper

## Setup your environment

Clone the repository:

```bash
git clone https://github.com/MDverse/mdws.git
```

Move to the new directory:

```bash
cd mdws
```

Install [miniconda](https://docs.conda.io/en/latest/miniconda.html).

Install [mamba](https://github.com/mamba-org/mamba):

```bash
conda install mamba -n base -c conda-forge
```

Create the `mdws` conda environment:
```
mamba env create -f binder/environment.yml
```

Load the `mdws` conda environment:
```
conda activate mdws
```

Note: you can also update the conda environment with:

```bash
mamba env update -f binder/environment.yml
```

To deactivate an active environment, use

```
conda deactivate
```

## Scrap Zenodo

Create a token here: <https://zenodo.org/account/settings/applications/tokens/new/>  
and store it in the file `.env`:
```
ZENODO_TOKEN=YOUR-ZENODO-TOKEN
```
This file is ignored by git.

Scrap Zenodo for MD-related datasets and files:

```bash
python scripts/scrap_zenodo.py -q params/query.yml -o data
```

Scrap Zenodo with a small query, for development or demo purpose:

```bash
python scripts/scrap_zenodo.py -q params/query_dev.yml -o test
```

The scraping takes some time. A mechanism has been set up to avoid overloading the Zenodo API. Be patient.

Eventually, the scraper will produce three files: `zenodo_datasets.tsv`, `zenodo_datasets_text.tsv` and `zenodo_files.tsv` :sparkles: 


## Scrap FigShare

Scrap FigShare for MD-related datasets and files:

```bash
python scripts/scrap_figshare.py -q params/query.yml -o data
```

Scrap FigShare with a small query, for development or demo purpose:

```bash
python scripts/scrap_figshare.py -q params/query_dev.yml -o test
```

The scraping takes some time (complete query: 20 min-120 min). Be patient.

Eventually, the scraper will produce three files: `figshare_datasets.tsv`, `figshare_datasets_text.tsv` and `figshare_files.tsv` :sparkles: 


## Scrap OSF

Create a token here: <https://osf.io/settings/tokens> (select the `osf.full_read` scope)
and store it in the file `.env`:
```
OSF_TOKEN=<YOUR OSF TOKEN HERE>
```
This file is ignored by git.

Scrap OSF for MD-related datasets and files:

```bash
python scripts/scrap_osf.py -q params/query.yml -o data
```

Scrap OSF with a small query, for development or demo purpose:

```bash
python scripts/scrap_osf.py -q params/query_dev.yml -o test
```

The scraping takes some time (~ 25 min). Be patient.

Eventually, the scraper will produce three files: `osf_datasets.tsv`, `osf_datasets_text.tsv` and `osf_files.tsv` :sparkles: 


## Analyse data

Run all Jupyter notebooks in batch mode:
```
jupyter nbconvert --to html  --execute --allow-errors --output-dir results notebooks/analyze_zenodo.ipynb
jupyter nbconvert --to html  --execute --allow-errors --output-dir results notebooks/zenodo_stats.ipynb
jupyter nbconvert --to html  --execute --allow-errors --output-dir results notebooks/search_MD_in_pubmed.ipynb
cp notebooks/*.{svg,png} results/
```


## Analyze Gromacs mdp and gro files

### Download files

To download Gromacs mdp and gro files, use the following commands:

```bash
python scripts/download_files.py -i data/zenodo_files.tsv -o data/downloads/ -t mdp -t gro
python scripts/download_files.py -i data/figshare_files.tsv -o data/downloads/ -t mdp -t gro
python scripts/download_files.py -i data/osf_files.tsv -o data/downloads/ -t mdp -t gro
```

To download Gromacs mdp and gro files also from zip files, use the `--includezipfiles` option:

```bash
python scripts/download_files.py -i data/zenodo_files.tsv -o data/downloads/ -t mdp -t gro --includezipfiles
python scripts/download_files.py -i data/figshare_files.tsv -o data/downloads/ -t mdp -t gro --includezipfiles
python scripts/download_files.py -i data/osf_files.tsv -o data/downloads/ -t mdp -t gro --includezipfiles
```

This step will take a couple of hours to complete. Depending on the stability of your internet connection and the availability of the data repository servers, the download might fail for a couple of files. Re-rerun previous commands to resume the download. Files already retrieved will not be downloaded again.

Expect about 15 GB of data without zip archives and 430 GB with zip archives.

To count the number of files, could do use:

```bash
find data/downloads -name *.gro | wc -l
```

or

```bash
find data/downloads -name *.mdp | wc -l
```

Numbers are indicative only.

### Parse files

```bash
python scripts/parse_mdp_files.py -i data/downloads -o data
python scripts/parse_gro_files.py -i data/downloads -r params/residue_names.yml -o data
```


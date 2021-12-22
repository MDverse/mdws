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

Install [mamba](https://github.com/mamba-org/mamba) :

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

## Add API keys

### Zenodo

Create a token here: <https://zenodo.org/account/settings/applications/tokens/new/>  
and store it in the file `.env`:
```
ZENODO_TOKEN=YOUR-ZENODO-TOKEN
```
This file is ignored by git.

## Scrap Zenodo

Scrap Zenodo for MD-related datasets and files:

```bash
python scrap_zenodo.py query.yml
```

Scrap Zenodo with a small query, for development or demo purpose:

```bash
python scrap_zenodo.py query_dev.yml
```

The scraping takes some time. A mechanism has been set up to avoid overloading the Zenodo API. Be patient.

Eventually, the scraper will produce two files: `zenodo_datasets.tsv` and `zenodo_files.tsv` :sparkles: 
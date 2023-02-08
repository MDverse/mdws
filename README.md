# Molecular Dynamics web scrapper

## Setup your environment

Install [miniconda](https://docs.conda.io/en/latest/miniconda.html).

Install [mamba](https://github.com/mamba-org/mamba):

```bash
conda install mamba -n base -c conda-forge
```

Clone this repository:

```bash
git clone https://github.com/MDverse/mdws.git
```

Move to the new directory:

```bash
cd mdws
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

To deactivate an conda active environment, use

```
conda deactivate
```

## Scrap Zenodo

Have a look to the notes regarding [Zenodo](docs/zenodo.md) and its API.

Create a token here: <https://zenodo.org/account/settings/applications/tokens/new/>  
and store it in the file `.env`:
```
ZENODO_TOKEN=YOUR-ZENODO-TOKEN
```
This file is ignored by git and won't be published on GitHub.

Scrap Zenodo for MD-related datasets and files:

```bash
python scripts/scrap_zenodo.py --query params/query.yml --output data
```

Scrap Zenodo with a small query, for development or demo purpose:

```bash
python scripts/scrap_zenodo.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (about an hour). A mechanism has been set up to avoid overloading the Zenodo API. Be patient.

Eventually, the scraper will produce three files: `zenodo_datasets.tsv`, `zenodo_datasets_text.tsv` and `zenodo_files.tsv` :sparkles:

Note that "[false positives](docs/false_positives.md)" have been removed in the scraping proccess.


## Scrap FigShare

Have a look to the notes regarding [Figshare](docs/figshare.md) and its API.

Scrap FigShare for MD-related datasets and files:

```bash
python scripts/scrap_figshare.py --query params/query.yml --output data
```

Scrap FigShare with a small query, for development or demo purpose:

```bash
python scripts/scrap_figshare.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (about 2 hours). Be patient.

Eventually, the scraper will produce three files: `figshare_datasets.tsv`, `figshare_datasets_text.tsv` and `figshare_files.tsv` :sparkles: 


## Scrap OSF

Have a look to the notes regarding [OSF](docs/osf.md) and its API.

Create a token here: <https://osf.io/settings/tokens> (select the `osf.full_read` scope)
and store it in the file `.env`:
```
OSF_TOKEN=<YOUR OSF TOKEN HERE>
```
This file is ignored by git and won't be published on GitHub.

Scrap OSF for MD-related datasets and files:

```bash
python scripts/scrap_osf.py --query params/query.yml --output data
```

Scrap OSF with a small query, for development or demo purpose:

```bash
python scripts/scrap_osf.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (~ 30 min). Be patient.

Eventually, the scraper will produce three files: `osf_datasets.tsv`, `osf_datasets_text.tsv` and `osf_files.tsv` :sparkles: 

## Analyze Gromacs mdp and gro files

### Download files

To download Gromacs mdp and gro files, use the following commands:

```bash
python scripts/download_files.py --input data/zenodo_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
python scripts/download_files.py --input data/figshare_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
python scripts/download_files.py --input data/osf_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

Option `--withzipfiles` will also get files packaged in zip archives. It means that the script will first download the entire zip archive and then extract the mdp and gro files.

This step will take a couple of hours to complete. Depending on the stability of your internet connection and the availability of the data repository servers, the download might fail for a couple of files. Re-rerun previous commands to resume the download. Files already retrieved will not be downloaded again.

Expect about 640 GB of data with the `--withzipfiles` option (~ 8800 gro files and 9500 mdp files)

Numbers are indicative only and may vary dependy on the time you run this command.

### Parse files

```bash
python scripts/parse_mdp_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --output data
```

This step will take a couple of seconds to run. Results will be saved in `data/gromacs_mdp_files_info.tsv`.


```bash
python scripts/parse_gro_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --residues params/residue_names.yml --output data
```

This step will take about 4 hours to run. Results will be saved in `data/gromacs_gro_files_info.tsv`.


## Run all script

You can run all commands above with the `run_all.sh` script:

```bash
bash run_all.sh
```

Be sure, you have have enhough time, bandwidth and disk space to run this command.
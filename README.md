# MDverse web scrapper (MDWS)

Parquet files and codebook are available on Zenodo: [10.5281/zenodo.7856523](https://doi.org/10.5281/zenodo.7856523)

## Setup your environment

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

Clone this repository:

```bash
git clone https://github.com/MDverse/mdws.git
```

> [!TIP]
> For maintainers only:
> ```bash
> git clone git@github.com:MDverse/mdws.git
> ```

Move to the new directory:

```bash
cd mdws
```

Create a virtual environment:

```bash
uv sync
```

## Scrape Zenodo

Have a look to the notes regarding [Zenodo](docs/zenodo.md) and its API.

Create a token here: <https://zenodo.org/account/settings/applications/tokens/new/>  
and store it in the file `.env`:

```
ZENODO_TOKEN=YOUR-ZENODO-TOKEN
```

This file is automatically ignored by git and won't be published on GitHub.

Scrape Zenodo for MD-related datasets and files:

```bash
uv run scripts/scrape_zenodo.py --query params/query.yml --output data
```

Scrape Zenodo with a small query, for development or demo purpose:

```bash
uv run scripts/scrape_zenodo.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (about an hour). A mechanism has been set up to avoid overloading the Zenodo API. Be patient.

Eventually, the scraper will produce three files: `zenodo_datasets.tsv`, `zenodo_datasets_text.tsv` and `zenodo_files.tsv` :sparkles:

Note that "[false positives](docs/false_positives.md)" have been removed in the scraping proccess.


## Scrape FigShare

Have a look to the notes regarding [Figshare](docs/figshare.md) and its API.

Scrape FigShare for MD-related datasets and files:

```bash
uv run scripts/scrape_figshare.py --query params/query.yml --output data
```

Scrape FigShare with a small query, for development or demo purpose:

```bash
uv run scripts/scrape_figshare.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (about 2 hours). Be patient.

Eventually, the scraper will produce three files: `figshare_datasets.tsv`, `figshare_datasets_text.tsv` and `figshare_files.tsv` :sparkles: 


## Scrape OSF

Have a look to the notes regarding [OSF](docs/osf.md) and its API.

Create a token here: <https://osf.io/settings/tokens> (select the `osf.full_read` scope)
and store it in the file `.env`:

```
OSF_TOKEN=<YOUR OSF TOKEN HERE>
```

This file is ignored by git and won't be published on GitHub.

Scrape OSF for MD-related datasets and files:

```bash
uv run scripts/scrape_osf.py --query params/query.yml --output data
```

Scrape OSF with a small query, for development or demo purpose:

```bash
uv run scripts/scrape_osf.py --query params/query_dev.yml --output tmp
```

The scraping takes some time (~ 30 min). Be patient.

Eventually, the scraper will produce three files: `osf_datasets.tsv`, `osf_datasets_text.tsv` and `osf_files.tsv` :sparkles: 


## Scrape NOMAD

Scrape Nomad for MD-related datasets and files:

```bash
uv run -m scripts.scrape_nomad
```
   
This command will:
   1. Fetch molecular dynamics entries from the NOMAD API in batches of 50.
   2. Parse their metadata and validate them using the Pydantic models
      `DatasetMetadata` and `FileMetadata`.
   3. Save validated dataset metadatas to `data/nomad/nomad_datasets.parquet`.
   4. Save validated file metadatas to `data/nomad/nomad_files.parquet`.

> The scraping takes less than 10 minutes.

## Scrape GPCRmd

Scrape GPCRmd to collect molecular dynamics (MD) datasets and files related to G-protein-coupled receptors (GPCRs), a major family of membrane proteins and common drug targets.

```bash
uv run -m scripts.scrape_gpcrmd
```

This command will:
   1. Fetch all available datasets from GPCRMD.
   2. Parse their metadata and validate them using the Pydantic models
    `DatasetMetadata` and `FileMetadata`.
   3. Save validated dataset metadatas to `data/gpcrmd/gpcrmd_datasets.parquet`.
   4. Save validated file metadatas to `data/gpcrmd/gpcrmd_files.parquet`.

> The scraping takes approximately 1 hour and 30 minutes.

## Scrape NMRLipids Databank

Scrape the NMRLipids Databank to extract metadata from molecular dynamics (MD) simulations.

1. Clone the NMRLipids repository

```bash
git clone https://github.com/NMRLipids/BilayerData.git
```

> All metadata are stored in `README.yaml` files under the `Simulations` directory.

2. Extract metadata from simulations

```bash
uv run scripts/scrape_nmrlipids.py \
  --sim-folder /path/to/BilayerData/Simulations
```

This command will:

1. Recursively search for all `README.yaml` files in the `Simulations` directory
2. Parse and normalize MD simulation metadata
3. Inject mandatory metadata fields (source, crawling_date, licence)
4. Validate entries using Pydantic models
5. Save the extracted metadata to Parquet files


## Analyze Gromacs mdp and gro files

### Download files

To download Gromacs mdp and gro files, use the following commands:

```bash
uv run scripts/download_files.py --input data/zenodo_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
uv run scripts/download_files.py --input data/figshare_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
uv run scripts/download_files.py --input data/osf_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

Option `--withzipfiles` will also get files packaged in zip archives. It means that the script will first download the entire zip archive and then extract the mdp and gro files.

This step will take a couple of hours to complete. Depending on the stability of your internet connection and the availability of the data repository servers, the download might fail for a couple of files. Re-rerun previous commands to resume the download. Files already retrieved will not be downloaded again.

Expect about 640 GB of data with the `--withzipfiles` option (~ 8800 gro files and 9500 mdp files)

Numbers are indicative only and may vary depend on the time you run this command (databases tend to get bigger and bigger).


### Parse .mdp files

```bash
uv run scripts/parse_mdp_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --output data
```

This step will take a couple of seconds to run. Results will be saved in `data/gromacs_mdp_files_info.tsv`.

### Parse .gro files

A rough molecular composition is deduced from the file `params/residue_name.yml` that contains a partial list of residues names organized in categories *protein*, *lipid*, *nucleic*, *glucid* and *water & ion*.

```bash
uv run scripts/parse_gro_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --residues params/residue_names.yml --output data
```

This step will take about 4 hours to run. Results will be saved in `data/gromacs_gro_files_info.tsv`.

### Export to Parquet

Parquet format is a column-based storage format that is supported by many data analysis tools.
It's an efficient data format for large datasets.

```bash
uv run scripts/export_to_parquet.py
```

This step will take a couple of seconds to run. Results will be saved in:

```bash
data/datasets.parquet
data/files.parquet
data/gromacs_gro_files.parquet
data/gromacs_mdp_files.parquet
```


## Run all script

You can run all commands above with the `run_all.sh` script:

```bash
bash run_all.sh
```

> [!WARNING]
> Be sure, you have have **sufficient** time, bandwidth and disk space to run this command.


## Upload data on Zenodo (for MDverse mainteners only)

*For the owner of the Zenodo record only. Zenodo token requires `deposit:actions` and `deposit:write` scopes.*

Update metadata:

```bash
uv run scripts/upload_datasets_to_zenodo.py --record 7856524 --metadata params/zenodo_metadata.json 
```

Update files:
```bash
uv run scripts/upload_datasets_to_zenodo.py --record 7856524 \
--file data/datasets.parquet \
--file data/files.parquet \
--file data/gromacs_gro_files.parquet \
--file data/gromacs_mdp_files.parquet \
--file docs/data_model_parquet.md
```

> [!NOTE]
> The latest version of the dataset is available with the DOI [10.5281/zenodo.7856523](https://zenodo.org/doi/10.5281/zenodo.7856523).

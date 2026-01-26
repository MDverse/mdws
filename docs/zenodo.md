# Zenodo documentation

## File size

According to Zenodo [FAQ](https://help.zenodo.org/):

> We currently accept up to 50GB per dataset (you can have multiple datasets); there is no size limit on communities.

So we don't expect much files to have an individual size above 50 GB.

## API

- Base URL: <https://zenodo.org/>
- [REST API](https://developers.zenodo.org/)
- List of [HTTP status codes](https://developers.zenodo.org/#http-status-codes)

### Token

Zenodo requires a token to access its API with higher rate limits. See "[Authentication](https://developers.zenodo.org/#authentication)" to get a token and "[Quickstart - Upload](https://developers.zenodo.org/#quickstart-upload)" to test it.

Example of direct API link for a given dataset: <https://zenodo.org/api/records/8183728>

### Query

[Search guide](https://help.zenodo.org/guides/search/)

### Rate limiting

The [rate limit](https://developers.zenodo.org/#rate-limiting) is

> 100 requests per minute, 5000 requests per hour

## Datasets

### Search of MD-related datasets

- Endpoint: `/api/records`
- HTTP method: GET
- Documentation: <https://developers.zenodo.org/#records>
- [Search guide](https://help.zenodo.org/guides/search/) and [documentation](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query) for the query string (Elastic)

Query examples:

```none
resource_type.type:"dataset" AND filetype:"tpr"
```

with keywords:

```none
resource_type.type:"dataset" AND filetype:"mdp"  AND ("molecular dynamics" OR "molecular dynamic" OR "molecular-dynamics" OR "molecular-dynamic" OR "md trajectory" OR "md trajectories" OR "md simulation" OR "md simulations" OR "gromacs" OR "gromos" OR "namd" OR "amber" OR "desmond" OR "amber96" OR "amber99" OR "amber14" OR "charmm" OR "charmm27" OR "charmm36" OR "martini")
```

### Search strategy

We search for all file types and keywords. Results are paginated by batch of 100 datasets.

The API send the full records of datasets, including complete files metadata.

### Get metadata for a given dataset

*For debugging purpose only, since all information is already provided in the search results*

- Endpoint: `/api/records/{dataset_id}`
- HTTP method: GET
- Documentation: <https://developers.zenodo.org/#records>

Example of datasets related to molecular dynamics:

- [Simulations of a beta-2 adrenergic receptor monomer on a flat membrane](https://zenodo.org/record/4114422) ([view in API](https://zenodo.org/api/records/4114422))
- [GROMACS simulations of unfolding of ubiqutin in a strong electric field](https://zenodo.org/record/4056037) ([view in API](https://zenodo.org/api/records/4056037))

### Zip files

Many MD simulation files are archived in zip files.

Query:

```none
resource_type.type:"dataset" AND filetype:"zip"  AND ("molecular dynamics" OR "molecular dynamic" OR "molecular-dynamics" OR "molecular-dynamic" OR "md trajectory" OR "md trajectories" OR "md simulation" OR "md simulations" OR "gromacs" OR "gromos" OR "namd" OR "amber" OR "desmond" OR "amber96" OR "amber99" OR "amber14" OR "charmm" OR "charmm27" OR "charmm36" OR "martini")
```

Example of datasets related to molecular dynamics with zip files:

- [All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E](https://zenodo.org/record/4743386)
- [Structural dynamics of DNA depending on methylation pattern: Simulation dataset](https://zenodo.org/record/3992686)
- [Exploring the interaction of a curcumin azobioisostere with Abeta42 dimers using replica exchange molecular dynamics simulations](https://zenodo.org/record/5573728)
- [Molecular dynamics simulation data of regulatory ACT domain dimer of human phenylalanine hydroxylase (PAH) (with unbound ligand)](https://zenodo.org/record/3814193) (with multiple zip files)

Some dataset cannot be found with keywords. For instance:

- [Ternary lipid composition in a curved geometry, more flat surface](https://zenodo.org/record/4644379)

#### Accessing zip file content

Zip file content can be accessed through a preview page provided by Zenodo.

The URL for zip file content preview is: <https://zenodo.org/record/{dataset_id}/preview/{zip_file_name}>

For dataset [All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E](https://zenodo.org/record/4743386) ([view in API](https://zenodo.org/api/records/4743386))

- preview for [NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip](https://zenodo.org/record/4743386/preview/NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip)
- preview for [NoPTM-4_POPC_CHARMM36m_0.1x3mks.zip](https://zenodo.org/record/4743386/preview/NoPTM-4_POPC_CHARMM36m_0.1x3mks.zip)

Note that the preview is available for the first 1000 files only.

File name and file size are the only metadata available from the preview.

#### Zip files with tree-like structure

Some zip file content are dense, with many folders and sub-folders.

Examples:

- For the dataset "[Input files and scripts for Hamiltonian replica-exchange molecular dynamics simulations of intrinsically disordered proteins using a software GROMACS patched with PLUMED](https://zenodo.org/record/4319228)": preview of the file [`hremd-idp.zip`](https://zenodo.org/record/4319228/preview/hremd-idp.zip).
- For the dataset "[2DUV Machine Learning Protocol Code](https://zenodo.org/record/4444751/)": preview of the file [`code.zip`](https://zenodo.org/record/4444751/preview/code.zip).

These complexe zip files are handled by the current implementation of the Zenodo scraper.

#### Issues with zip file content

Sometimes, zip file contents are not accessible.

For the dataset "[G-Protein Coupled Receptor-Ligand Dissociation Rates and Mechanisms from tauRAMD Simulations](https://zenodo.org/record/5151217)": preview of the file [`Example_b2AR-alprenolol.zip`](https://zenodo.org/record/5151217/preview/Example_b2AR-alprenolol.zip) is not available, probably because the file is too large (5.4 GB).

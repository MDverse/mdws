# ATLAS

ATLAS (Atlas of proTein moLecular dynAmicS) is an open-access data repository that gathers standardized molecular dynamics simulations of protein structures, accompanied by their analysis in the form of interactive diagrams and trajectory visualisation. All raw trajectories as well as the results of analysis are available for download.

- web site: <https://www.dsimb.inserm.fr/ATLAS/>
- publication: [ATLAS: protein flexibility description from atomistic molecular dynamics simulations](https://academic.oup.com/nar/article/52/D1/D384/7438909), Nucleic Acids Research, 2024.

## API

- Base URL: <https://www.dsimb.inserm.fr/ATLAS/api/>
- [documentation](https://www.dsimb.inserm.fr/ATLAS/api/redoc)

No account / token is needed to access ATLAS API.

### Datasets

In ATLAS, each dataset corresponds to a molecular dynamics simulation of a **protein chain** and is uniquely identified by a **PDB ID and chain identifier** (`pdb_chain`).

The list of all available datasets can be obtained from the ATLAS index page: <https://www.dsimb.inserm.fr/ATLAS/>

All datasets (pdb chains) are extracted from this page.

### Metadata for a given dataset

API endpoint to retrieve metadata for a given dataset:

- Endpoint: `/ATLAS/metadata/{pdb_chain}`
- HTTP method: GET
- documentation: <https://www.dsimb.inserm.fr/ATLAS/api/redoc>

This endpoint returns structured JSON metadata describing the simulated protein.

Example with dataset id `1k5n_A`:

- [web page](https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A.html)
- [API view](https://www.dsimb.inserm.fr/ATLAS/api/ATLAS/metadata/1k5n_A)

Remarks:

- The title of the dataset is the protein name.
- No comment or description is provided. We used the organism as description.

### Metadata for files

Files associated with a given dataset are hosted in a public directory.

For each dataset, 3 zip files are provided. They are accessible through the web page of each individual dataset: <https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{pdb_chain}/{pdb_chain}.html>

Zip files url follow these patterns:

- Analysis & MDs (1,000 frames, only protein): <https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{pdb_chain}/{pdb_chain}_analysis.zip>
- MDs (10,000 frames, only protein): <https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{pdb_chain}/{pdb_chain}_protein.zip>
- MDs (10,000 frames, total system): <https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{pdb_chain}/{pdb_chain}_total.zip>

Example with dataset id `1k5n_A`:

- [web page](https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A.html)
- [1k5n_A_analysis.zip](https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A_analysis.zip)
- [1k5n_A_protein.zip](https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A_protein.zip)
- [1k5n_A_total.zip](https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A_total.zip)

# ATLAS.

ATLAS (Atlas of proTein moLecular dynAmicS) is an open-access data repository that gathers standardized molecular dynamics simulations of protein structures, accompanied by their analysis in the form of interactive diagrams and trajectory visualisation. All raw trajectories as well as the results of analysis are available for download.

- web site: https://www.dsimb.inserm.fr/ATLAS/
- documentation: https://www.dsimb.inserm.fr/ATLAS/api/redoc
- API: https://www.dsimb.inserm.fr/ATLAS/api/

No account / token is needed to access ATLAS API.

---

## Finding molecular dynamics datasets and files

### Datasets

In ATLAS, each dataset corresponds to a molecular dynamics simulation of a **protein chain** and is uniquely identified by a **PDB ID and chain identifier** (`pdb_chain`).

The list of all available datasets can be obtained from the ATLAS HTML index:

https://www.dsimb.inserm.fr/ATLAS/

This page is used as the **discovery layer** to extract all available PDB chain identifiers.

---

### API entrypoint to search for entries

API endpoint to retrieve metadata for a given dataset:

- Path: `/ATLAS/metadata/{pdb_chain}`
- documentation: https://www.dsimb.inserm.fr/ATLAS/api/redoc

This endpoint returns structured JSON metadata describing the protein and its molecular dynamics simulation.

---

### Files

Files associated with a given dataset are hosted in a public directory.

- Base path: `/database/ATLAS/{pdb_chain}/`

These directories contain structure files (PDB, CIF), molecular dynamics trajectories, and precomputed analysis results.

---

## Examples

### 1k5n_A

- entry id: `1k5n_A`
- entry on ATLAS GUI: https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A.html
- entry on ATLAS API: https://www.dsimb.inserm.fr/ATLAS/api/ATLAS/metadata/1k5n_A

### Description (called "Comment") :

HLA class I histocompatibility antigen, B alpha chain

### Files

- files on ATLAS GUI: https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/1k5n_A/1k5n_A.html


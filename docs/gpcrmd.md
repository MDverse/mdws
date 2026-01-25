# GPCRmd

> GPCRmd is an online platform for visualizing, analyzing, and sharing molecular dynamics simulations of G-protein-coupled receptors (GPCRs), a key family of membrane proteins and common drug targets.

- web site: https://www.gpcrmd.org/
- documentation: https://gpcrmd-docs.readthedocs.io/en/latest/index.html
- API: https://www.gpcrmd.org/api/
  - `version v1.3`

No account / token is needed to access GPCRmd API.


## Finding molecular dynamics datasets and files

Although GPCRmd provides a public API to discover molecular dynamics datasets, **some important metadata fields and all file-level information are not exposed via the API**. For this reason, web scraping of the dataset page is required to retrieve complete dataset descriptions and file metadata.

### Datasets

In GPCRmd, datasets (a simulation and its related files) are called "dynamic".

API entrypoint to search for all datasets at once:

- Path: /search_all/info/
- [documentatation](https://gpcrmd-docs.readthedocs.io/en/latest/api.html#main-gpcrmd-api)


#### Dataset metadata retrieved via API:

| Field              | Description                         |
| ------------------ | ----------------------------------- |
| `dyn_id`           | *Unique dynamic (dataset) identifier* |
| `modelname`        | *Name of the simulated model*         |
| `timestep`         | *MD integration timestep*             |
| `atom_num`         | *Number of atoms*                     |
| `mysoftware`       | *MD engine used*                      |
| `software_version` | *Version of the MD engine*            |
| `forcefield`       | *Force field and model name*          |
| `forcefield_version` | *Force field and model version*     |
| `creation_timestamp` | *Dataset creation date*             |
| `dataset_url`        | *URL of the dataset web page*       |

#### Dataset metadata retrieved via web scraping (URL provided by the API):

| Field                | Description                                |
| -------------------- | ------------------------------------------ |
| `description`        | *Full textual description of the simulation* |
| `authors`            | *Dataset authors*                           |
| `simulation_time`    | *Total simulation length*                   |

 
### Files

The GPCRmd API does not provide any endpoint to access file-level metadata. All file information must therefore be extracted from the dataset web page. Two file categories are available: **simulation output files** and **simulation protocol and starting files**.

For example, the files corresponding to the dataset` 7` (https://www.gpcrmd.org/dynadb/dynamics/id/7/) include these files:
- https://www.gpcrmd.org/dynadb/files/Dynamics/10166_trj_7.dcd
- https://www.gpcrmd.org/dynadb/files/Dynamics/10167_dyn_7.psf
- https://www.gpcrmd.org/dynadb/files/Dynamics/10168_dyn_7.pdb


#### File metadata retrieved via web scraping (URL provided by the API):

| Field      | Description            |
| ---------- | ---------------------- |
| `file_name`  | *Name of the file*     |
| `file_type` | *File extension*      |
| `file_path`| *Public download URL*    |
| `file_size`| *File size in bytes*    |

> 💡 File size is obtained using an HTTP `HEAD` request on the file path, **avoiding file download**.


## Examples
### Dataset ID 2316

- [Dataset on GPCRmd GUI](https://www.gpcrmd.org/dynadb/dynamics/id/2316/)
- [Dataset on GPCRmd API](https://www.gpcrmd.org/api/search_dyn/info/2316)


#### Dataset metadata (API + scraping)

| Field              | Description                         |
| ------------------ | ----------------------------------- |
| `dyn_id`           | *2316* |
| `modelname`        | *FFA2_TUG1375_Gi1-TUG1375*        |
| `timestep`         | *2*             |
| `atom_num`         | *4829*                     |
| `mysoftware`       | *AMBER PMEMD.CUDA*                     |
| `software_version` | *2020*            |
| `forcefield`       | *ff19SB/lipid21/GAFF2*         |
| `forcefield_version` | *ff19SB/lipid21*     |
| `creation_timestamp` | *2025-05-13*            |
| `dataset_url`        | *https://www.gpcrmd.org/dynadb/dynamics/id/2316/*     |
| `description`        | *Simulation aims to observe structural features of FFA2 without an orthosteric agonist and G-protein, which will be compared to docking-based simulations of allosteric activators...* |
| `authors`            | *Abdul-Akim Guseinov, University of Glasgow*                           |
| `simulation_time`    | *3.0 µs*                   |


- [files on GPCRmd GUI](https://www.gpcrmd.org/api/search_dyn/info/2316) (accessible via the *Technical Information* section)

#### Example file from the dataset

| Field      | Description            |
| ---------- | ---------------------- |
| `file_name`  | *tmp_dyn_0_2667.pdb*    |
| `file_type` | *pdb*     |
| `file_path`| *https://www.gpcrmd.org/dynadb/files/Dynamics/dyn2667/tmp_dyn_0_2667.pdb* |
| `file_size`| *1 024 bytes*    |


## References 

Rodríguez-Espigares, I., Torrens-Fontanals, M., Tiemann, J.K.S. et al. GPCRmd uncovers the dynamics of the 3D-GPCRome. Nat Methods. 2020;17(8):777-787. doi:[10.1038/s41592-020-0884-y](https://www.nature.com/articles/s41592-020-0884-y)
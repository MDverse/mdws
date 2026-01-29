# GPCRmd

> GPCRmd is an online platform for visualizing, analyzing, and sharing molecular dynamics simulations of G-protein-coupled receptors (GPCRs), a key family of membrane proteins and common drug targets.

- web site: <https://www.gpcrmd.org/>
- publication: [GPCRmd uncovers the dynamics of the 3D-GPCRome](https://www.nature.com/articles/s41592-020-0884-y), Nature Methods, 2020.
- [documentation](https://gpcrmd-docs.readthedocs.io/en/latest/index.html)

## API

### Base URL

<https://www.gpcrmd.org/api/>

### Documentation

<https://gpcrmd-docs.readthedocs.io/en/latest/api.html#main-gpcrmd-api>

### Token

No token is needed to GPCRmd API in read mode.

### Metadata of datasets and files

Although GPCRmd provides a public API to discover molecular dynamics datasets, **some metadata fields and all file-level information are not exposed via the API**. For this reason, web scraping of the dataset page is required to retrieve complete dataset descriptions and file metadata.

### Datasets

In GPCRmd, dataset (a simulation and its related files) is called a "dynamic".

API entrypoint to search for all datasets at once:

- Endpoint: `/search_all/info/`
- HTTP method: GET
- [documentation](https://gpcrmd-docs.readthedocs.io/en/latest/api.html#main-gpcrmd-api)

#### Dataset metadata retrieved via the API

| Field                | Description                         |
| -------------------- | ----------------------------------- |
| `dyn_id`             | Unique dynamic (dataset) identifier |
| `modelname`          | Name of the simulated system        |
| `timestep`           | MD integration time step in fs      |
| `atom_num`           | Number of atoms                     |
| `mysoftware`         | MD engine used                      |
| `software_version`   | Version of the MD engine            |
| `forcefield`         | Force field and model name          |
| `forcefield_version` | Force field and model version       |
| `creation_timestamp` | Dataset creation date               |
| `dataset_url`        | URL of the dataset web page         |

#### Dataset metadata retrieved via scraping of the dataset HTML page

| Field                | Description                                |
| -------------------- | ------------------------------------------ |
| `description`        | Textual description of the simulation      |
| `authors`            | Authors                                    |
| `simulation_time`    | Total simulation length                    |

### Files

The GPCRmd API does not provide any endpoint to access file-level metadata. File metadata is extracted from the dataset web page.

For example:

- Files associated to the [dataset `7`](https://www.gpcrmd.org/dynadb/dynamics/id/7/) are:

  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10166_trj_7.dcd>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10167_dyn_7.psf>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10168_dyn_7.pdb>

- Files associated to the [dataset `12`](https://www.gpcrmd.org/dynadb/dynamics/id/12/) are:

  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10193_trj_12.xtc>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10196_dyn_12.psf>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10197_dyn_12.pdb>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10191_oth_12.tar.gz>
  - <https://www.gpcrmd.org/dynadb/files/Dynamics/10192_prm_12.tar.gz>

#### File metadata retrieved via scraping of the dataset HTML page

| Field       | Description            |
| ----------- | ---------------------- |
| `file_name` | *Name of the file*     |
| `file_type` | *File extension*       |
| `file_path` | *Public download URL*  |
| `file_size` | *File size in bytes*   |

> 💡 File size is obtained using an HTTP `HEAD` request on the file URL, **avoiding file download**.

## Example

### Dataset ID 2316

- [Dataset on GPCRmd GUI](https://www.gpcrmd.org/dynadb/dynamics/id/2316/)
- [Dataset on GPCRmd API](https://www.gpcrmd.org/api/search_dyn/info/2316)

#### Dataset metadata (API + scraping)

| Field                | Description                                       |
| -------------------- | ------------------------------------------------- |
| `dyn_id`             | 2316                                              |
| `modelname`          | FFA2_TUG1375_Gi1-TUG1375                          |
| `timestep`           | 2                                                 |
| `atom_num`           | 4829                                              |
| `mysoftware`         | AMBER PMEMD.CUDA                                  |
| `software_version`   | 2020                                              |
| `forcefield`         | ff19SB/lipid21/GAFF2                              |
| `forcefield_version` | ff19SB/lipid21                                    |
| `creation_timestamp` | 2025-05-13                                        |
| `dataset_url`        | <https://www.gpcrmd.org/dynadb/dynamics/id/2316/> |
| `description`        | Simulation aims to observe structural features of FFA2 without an orthosteric agonist and G-protein, which will be compared to docking-based simulations of allosteric activators... |
| `authors`            | Abdul-Akim Guseinov, University of Glasgow        |
| `simulation_time`    | 3.0 µs                                            |

#### Files metadata

[files on GPCRmd GUI](https://www.gpcrmd.org/api/search_dyn/info/2316) (accessible via the *Technical Information* section)

| Field       | Description                                                               |
| ----------- | ------------------------------------------------------------------------- |
| `file_name` | tmp_dyn_0_2667.pdb                                                        |
| `file_path` | <https://www.gpcrmd.org/dynadb/files/Dynamics/dyn2667/tmp_dyn_0_2667.pdb> |
| `file_size` | 1024                                                                      |

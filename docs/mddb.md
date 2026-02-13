# MDDB

> The [MDDB (Molecular Dynamics Data Bank) project](https://mddbr.eu/about/) is an initiative to collect, preserve, and share molecular dynamics (MD) simulation data.
As part of this project, **MDposit** is an open platform that provides web access to atomistic MD simulations.
Its goal is to facilitate and promote data sharing within the global scientific community to advance research.

The MDposit infrastructure is distributed across several MDposit nodes. All metadata are accessible through the global node:

MDposit MMB node:

- web site: <https://mdposit.mddbr.eu/>
- documentation: <https://mdposit.mddbr.eu/#/help>
- API: <https://mdposit.mddbr.eu/api/rest/docs/>
- API base URL: <https://mdposit.mddbr.eu/api/rest/v1>

No account / token is needed to access the MDposit API.

## Getting metadata

### Datasets

In MDposit, a dataset (a simulation and its related files) is called a "[project](https://mdposit.mddbr.eu/api/rest/docs/#/projects/get_projects_summary)".

API entrypoint to get the total number of projects:

- Endpoint: `/projects/summary`
- HTTP method: GET
- [documentation](https://mdposit.mddbr.eu/api/rest/docs/#/projects/get_projects_summary)

A project can contain multiple replicas, each identified by `project_id`.`replica_id`.

For example, the project [MD-A003ZP](https://mdposit.mddbr.eu/#/id/MD-A003ZP/overview) contains ten replicas:

- `MD-A003ZP.1`: <https://mdposit.mddbr.eu/#/id/MD-A003ZP.1/overview>
- `MD-A003ZP.2`: <https://mdposit.mddbr.eu/#/id/MD-A003ZP.2/overview>
- `MD-A003ZP.3`: <https://mdposit.mddbr.eu/#/id/MD-A003ZP.3/overview>
- ...

API entrypoint to get all datasets at once:

- Endpoint: `/projects`
- HTTP method: GET
- [documentation](https://mdposit.mddbr.eu/api/rest/docs/#/projects/get_projects)

### Files

API endpoint to get files for a given replica of a project:

- Endpoint: `/projects/{project_id.replica_id}/filenotes`
- HTTP method: GET
- [documentation](https://mdposit.mddbr.eu/api/rest/docs/#/filenotes/get_projects__projectAccessionOrID__filenotes)

## Examples

### Project `MD-A003ZP`

Title:

> MDBind 3x1k

Description:

> 10 ns simulation of 1ma4m pdb structure from MDBind dataset, a dynamic view of the PDBBind database

- [project on MDposit GUI](https://mdposit.mddbr.eu/#/id/MD-A003ZP/overview)
- [project on MDposit API](https://mdposit.mddbr.eu/api/rest/current/projects/MD-A003ZP)

Files for replica 1:

- [files on MDposit GUI](https://mdposit.mddbr.eu/#/id/MD-A003ZP.1/files)
- [files on MDposit API](https://mdposit.mddbr.eu/api/rest/current/projects/MD-A003ZP.1/filenotes)

### Project `MD-A001T1`

Title:

> All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E in the monomeric form, C4 popc

Description:

> The trajectories of all-atom MD simulations were obtained based on 4 starting representative conformations from the CG simulation.
For each starting structure, there are six trajectories of the E protein: 3 with the protein embedded in the membrane containing POPC, and 3 with the membrane mimicking the natural ERGIC membrane
(Mix: 50% POPC, 25% POPE, 10% POPI, 5% POPS, 10% cholesterol).

- [project on MDposit GUI](https://mdposit.mddbr.eu/#/id/MD-A001T1/overview)
- [project on MDposit API](https://mdposit.mddbr.eu/api/rest/current/projects/MD-A001T1)

Files for replica 1:

- [files on MDposit GUI](https://mdposit.mddbr.eu/#/id/MD-A001T1.1/files)
- [files on MDposit API](https://mdposit.mddbr.eu/api/rest/current/projects/MD-A001T1.1/filenotes)

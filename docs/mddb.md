# MDDB

> The [MDDB (Molecular Dynamics Data Bank) project](https://mddbr.eu/about/) is an initiative to collect, preserve, and share molecular dynamics (MD) simulation data. As part of this project, **MDposit** is an open platform that provides web access to atomistic MD simulations. Its goal is to facilitate and promote data sharing within the global scientific community to advance research.

The MDDB infrastructure is distributed across **two MDposit nodes**. Both nodes expose the same REST API entry points. The only difference is the base URL used to access the API.

## MDposit MMB node

- web site: <https://mmb-dev.mddbr.eu/#/browse>
- documentation: <https://mmb.mddbr.eu/#/help>
- API: <https://mmb.mddbr.eu/api/rest/docs/>
- API base URL: <https://mmb.mddbr.eu/api/rest/v1>

## MDposit INRIA node

- web site: <https://dynarepo.inria.fr/#/browse>
- documentation: <https://dynarepo.inria.fr/#/help>
- API: <https://dynarepo.inria.fr/api/rest/docs/>
- API base URL: <https://inria.mddbr.eu/api/rest/v1>


No account / token is needed to access the MDposit API.

## Finding molecular dynamics datasets and files

### Datasets

In MDposit, a dataset (a simulation and its related files) is called an "[project](https://mmb.mddbr.eu/api/rest/docs/#/projects/get_projects_summary)" and a project can contain multiple replicas, each identified by `project_id`.`replica_id`.


For example, the project [A026F](https://mmb.mddbr.eu/#/id/A026F/overview) contains four replicas:
  - `A026F.1`: https://mmb.mddbr.eu/#/id/A026F.1/overview
  - `A026F.2`: https://mmb.mddbr.eu/#/id/A026F.2/overview
  - `A026F.3`: https://mmb.mddbr.eu/#/id/A026F.3/overview
  - `A026F.4`: https://mmb.mddbr.eu/#/id/A026F.4/overview


API entrypoint to search for all datasets at once:

- Endpoint: `/projects`
- HTTP method: GET
- [documentation](https://mmb.mddbr.eu/api/rest/docs/#/projects/get_projects)


### Files

API endpoint to get files for a given replica of a project:

- Endpoint: `/projects/{project_id.replica_id}/filenotes`
- HTTP method: GET
- [documentation](https://mmb.mddbr.eu/api/rest/docs/#/filenotes/get_projects__projectAccessionOrID__filenotes)

## Examples

### Project `A026F`

- Project id: `A026F.1`
- [project on MDposit GUI](https://mmb.mddbr.eu/#/id/A026F.1/overview)
- [project on MDposit API](https://mmb.mddbr.eu/api/rest/current/projects/A026F.1)

Description:

> Multi-scale simulation approaches which couple the molecular and neuronal simulations to predict the variation in the membrane potential and the neural spikes.

- [files on MDposit GUI](https://mmb.mddbr.eu/#/id/A026F.1/files)
- [files on MDposit API](https://mmb.mddbr.eu/api/rest/current/projects/A026F.1/filenotes)

### Project `A025U`

- Project id: `A025U.1`
- [project on MDposit GUI](https://mmb.mddbr.eu/#/id/A025U/overview)
- [project on MDposit API](https://mmb.mddbr.eu/api/rest/current/projects/A025U.2)

Remark: no description is provided for this dataset.

- [files on MDposit GUI](https://mmb.mddbr.eu/#/id/A025U/files)
- [files on MDposit API](https://mmb.mddbr.eu/api/rest/current/projects/A025U.2/filenotes)

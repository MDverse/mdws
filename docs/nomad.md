# NOMAD

> NOMAD is a free, and open-source data management platform for materials science, whose goal is to make scientific research data FAIR (findable, accessible, interoperable and reusable).

- web site: <https://nomad-lab.eu/nomad-lab/>
- documentation: <https://nomad-lab.eu/prod/v1/docs/api.html>
- API: <https://nomad-lab.eu/prod/v1/api/v1/>

No account / token is needed to access the NOMAD API.

## Finding molecular dynamics datasets and files

### Datasets

In NOMAD, a dataset (a simulation and its related files) is called an "[entry](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries/metadata)" and a dataset is a set of multiple entries.

Entries related to molecular dynamics are found with the query terms "`workflow name = MolecularDynamics`": <https://nomad-lab.eu/prod/v1/gui/search/entries?results.method.workflow_name=MolecularDynamics>

API endpoint to search for entries related to MD:

- Endpoint: `/entries/query`
- HTTP method: POST
- [documentation](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries%2Fmetadata/post_entries_metadata_query_entries_query_post)

Payload example:

```json
{
    "owner": "visible",
    "query": {"results.method.workflow_name:any": ["MolecularDynamics"]},
    "aggregations": {},
    "pagination": {
        "order_by": "upload_create_time",
        "order": "desc",
        "page_size": 10,
        "page_after_value":None,
    },
    "required": {
        "exclude": [
            "quantities",
            "sections",
        ]
    },
}
```

We use pagination to get all results from this query. All dataset metadata are collected at this stage.

For debugging purpose, one want to get metadata for a given dataset:

- Endpoint: `/entries/{entry_id}`
- HTTP method: GET
- [documentation](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries%2Fmetadata/post_entries_metadata_query_entries_query_post)

### Files

API endpoint to get files for a given entry:

- Endpoint: `/entries/{entry_id}/rawdir`
- HTTP method: GET
- [documentation](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries%2Fmetadata/get_entry_metadata_entries__entry_id__get)

## Examples

### Entry `x7sBrcHMgkcZcurdULy_qlxnnf6K`

- entry id: `x7sBrcHMgkcZcurdULy_qlxnnf6K`
- [entry on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/x7sBrcHMgkcZcurdULy_qlxnnf6K)
- [entry on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/x7sBrcHMgkcZcurdULy_qlxnnf6K)

Description (called "Comment"):

> MD simulations of wastewater pollutants on amorphous TiO2 at pH 7.4 in TIP3P water with NaCl ions. One simulation is calculated without pollutants, but with additional salt. Originally, the trajectories contain a total of 100 ns simulation time with a 10 ps time step. To compact the size, uploaded trajectories have a lower frequency of 100 ps time steps. If original trajectories are of interest, please contact the author. The two different topol.top files are for different stages in the equilibrium and production run: topol_I.top for the first equilibrium steps and topol_II.top for later equilibrium and production. They have different position restraints applied.

- [files on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/x7sBrcHMgkcZcurdULy_qlxnnf6K/files/)
- [files on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/x7sBrcHMgkcZcurdULy_qlxnnf6K/rawdir)

### Entry `wZib4jUWPP6TMTddN-POCI9Oko82`

- entry id: `wZib4jUWPP6TMTddN-POCI9Oko82`
- [entry on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/wZib4jUWPP6TMTddN-POCI9Oko82)
- [entry on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/wZib4jUWPP6TMTddN-POCI9Oko82)

Remark: no description is provided for this dataset.

- [files on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/wZib4jUWPP6TMTddN-POCI9Oko82/files/)
- [files on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/wZib4jUWPP6TMTddN-POCI9Oko82/rawdir)

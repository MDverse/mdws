# NOMAD

> NOMAD is a free, and open-source data management platform for materials science, whose goal is to make scientific research data FAIR (findable, accessible, interoperable and reusable).

- web site: https://nomad-lab.eu/nomad-lab/
- documentation: https://nomad-lab.eu/prod/v1/docs/api.html
- API: https://nomad-lab.eu/prod/v1/api/v1/

No account / token is needed to access NOMAD API.

## Finding molecular dynamics datasets and files

### Datasets

In NOMAD, datasets (a simulation and its related files) are called "entry" and datasets are sets of multiple entries.

Entries related to molecular dynamics corresponds to "workflow name = MolecularDynamics": https://nomad-lab.eu/prod/v1/gui/search/entries?results.method.workflow_name=MolecularDynamics

API entrypoint to search for entries:

- Path: `/entries/rawdir/query`
- [documentatation](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries%2Fraw/post_entries_rawdir_query_entries_rawdir_query_post)

### Files

API endpoint to get files for a given entry:
- Path: `/entries/{entry_id}/rawdir`
- [documentation](https://nomad-lab.eu/prod/v1/api/v1/extensions/docs#/entries%2Fraw/get_entry_rawdir_entries__entry_id__rawdir_get)

## Examples

### x7sBrcHMgkcZcurdULy_qlxnnf6K

- entry id: x7sBrcHMgkcZcurdULy_qlxnnf6K
- [entry on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/x7sBrcHMgkcZcurdULy_qlxnnf6K)
- [entry on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/x7sBrcHMgkcZcurdULy_qlxnnf6K)

Description (called "Comment") :

> MD simulations of wastewater pollutants on amorphous TiO2 at pH 7.4 in TIP3P water with NaCl ions. One simulation is calculated without pollutants, but with additional salt. Originally, the trajectories contain a total of 100 ns simulation time with a 10 ps time step. To compact the size, uploaded trajectories have a lower frequency of 100 ps time steps. If original trajectories are of interest, please contact the author. The two different topol.top files are for different stages in the equilibrium and production run: topol_I.top for the first equilibrium steps and topol_II.top for later equilibrium and production. They have different position restraints applied.

- [files on NOMAD GUI](https://nomad-lab.eu/prod/v1/gui/search/entries/entry/id/x7sBrcHMgkcZcurdULy_qlxnnf6K/files/)
- [files on NOMAD API](https://nomad-lab.eu/prod/v1/api/v1/entries/x7sBrcHMgkcZcurdULy_qlxnnf6K/rawdir)

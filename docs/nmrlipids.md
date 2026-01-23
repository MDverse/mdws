# NMRLipids Databank

> **NMRLipids (FAIRMD Lipids)** is an open-access data repository that collects, curates, and standardizes molecular dynamics (MD) simulations of lipid bilayers.  
> The databank provides harmonized simulation metadata, raw trajectories, and derived analysis results, following FAIR data principles.

All simulation metadata are stored in structured `README.yaml` files, enabling automated metadata extraction and large-scale analyses.

- **web site:** https://www.databank.nmrlipids.fi/
- **documentation:** https://nmrlipids.github.io/FAIRMD_lipids/latest/
- **reference publication:** https://www.nature.com/articles/s41467-024-45189-z

No account / token is needed to access the NMRLipids databank or its metadata repositories.

---

## Finding molecular dynamics datasets and files

### Datasets

In NMRLipids, each dataset corresponds to a **single molecular dynamics simulation of a lipid bilayer system**.  
Each simulation is uniquely identified by an integer **trajectory ID** (`ID`), which is used consistently across the databank.

The trajectory ID is the primary identifier used to:
- retrieve structured metadata,
- access the simulation through the web UI,
- link to external repositories hosting the raw simulation files.

Example trajectory page:
- https://databank.nmrlipids.fi/trajectories/{ID}

---

### Metadata discovery layer

Simulation metadata are stored in the **NMRLipids BilayerData GitHub repository**, where each simulation is described by a `README.yaml` file.

- **metadata repository:** https://github.com/NMRLipids/BilayerData
- **metadata location:**  
  `BilayerData/Simulations/**/README.yaml`

These `README.yaml` files constitute the **authoritative source of metadata** for the NMRLipids databank and are used as the discovery layer by the scraper.

---

## Files

Simulation files (trajectories, structures, and analysis outputs) are **not stored directly in the BilayerData repository**.

Instead, raw simulation data are typically hosted in **external archival repositories**, most commonly **Zenodo**, and are referenced from the metadata via ource ids.

Access to files is provided through:
- links available on the NMRLipids web UI,
- external repositories referenced in the metadata.

Example web entry:
- https://databank.nmrlipids.fi/trajectories/{ID}

From this page, users can:
- inspect simulation metadata,
- download structure files,
- follow links to the full simulation data hosted on Zenodo.

---

## Examples

### Trajectory ID 123

- **entry ID:** 123
- **entry on NMRLipids Web UI:**  
  https://databank.nmrlipids.fi/trajectories/123

The web page displays key simulation metadata (force field, temperature, simulation length, system composition) and provides links to the full simulation files hosted externally (e.g. Zenodo).


# Zenodo documentation

## File size

According to Zenodo [FAQ](https://help.zenodo.org/):

> We currently accept up to 50GB per dataset (you can have multiple datasets); there is no size limit on communities.

So we don't expect much files to have an individual size above 50 GB.

## API

[REST API](https://developers.zenodo.org/)

The [rate limit](https://developers.zenodo.org/#rate-limiting) is 
> 100 requests per minute, 5000 requests per hour

List of [HTTP status codes](https://developers.zenodo.org/#http-status-codes)

## Query

[Search guide](https://help.zenodo.org/guides/search/)

## Dataset examples

### MD-related file types

Query:
```
resource_type.type:"dataset" AND filetype:"tpr"
```

Datasets:

- [Simulations of a beta-2 adrenergic receptor monomer on a flat membrane](https://zenodo.org/record/4114422)
- [GROMACS simulations of unfolding of ubiqutin in a strong electric field](https://zenodo.org/record/4056037)


### zip files

Query:
```
resource_type.type:"dataset" AND filetype:"zip" AND (simulation* "molecular dynamics" gromacs charmm namd)
```

Datasets:

- [All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E](https://zenodo.org/record/4743386)
- [Structural dynamics of DNA depending on methylation pattern: Simulation dataset](https://zenodo.org/record/3992686)
- [Exploring the interaction of a curcumin azobioisostere with Abeta42 dimers using replica exchange molecular dynamics simulations](https://zenodo.org/record/5573728)
- [Molecular dynamics simulation data of regulatory ACT domain dimer of human phenylalanine hydroxylase (PAH) (with unbound ligand)](https://zenodo.org/record/3814193) (with multiple zip files)


Some dataset cannot be found with keywords. For instance:

- [Ternary lipid composition in a curved geometry, more flat surface](https://zenodo.org/record/4644379)

#### Accessing zip file content

Zip file content can be obtained through a preview page provided by Zenodo.

Example dataset: [All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E](https://zenodo.org/record/4743386)

- preview for [NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip](https://zenodo.org/record/4743386/preview/NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip)
- preview for [NoPTM-4_POPC_CHARMM36m_0.1x3mks.zip](https://zenodo.org/record/4743386/preview/NoPTM-4_POPC_CHARMM36m_0.1x3mks.zip)

#### Issues with zip file content

Some zip file content are really dense, with a directory-like organization.

Examples:

- In the dataset "[Input files and scripts for Hamiltonian replica-exchange molecular dynamics simulations of intrinsically disordered proteins using a software GROMACS patched with PLUMED](https://zenodo.org/record/4319228)", a preview of the file `hremd-idp.zip` is available [here](https://zenodo.org/record/4319228/preview/hremd-idp.zip).
- In the dataset "[2DUV Machine Learning Protocol Code](https://zenodo.org/record/4444751/)", a preview of the file `code.zip` is available [here](https://zenodo.org/record/4444751/preview/code.zip).

Sometimes, zip file contents are not accessible.

Example dataset: [G-Protein Coupled Receptor-Ligand Dissociation Rates and Mechanisms from tauRAMD Simulations](https://zenodo.org/record/5151217)

- preview not available for [Example_b2AR-alprenolol.zip](https://zenodo.org/record/5151217/preview/Example_b2AR-alprenolol.zip) probably because file is too large (5.4 GB)

# Zenodo documentation

[REST API](https://developers.zenodo.org/)

The [rate limit](https://developers.zenodo.org/#rate-limiting) is 
> 100 requests per minute, 5000 requests per hour

[Search guide](https://help.zenodo.org/guides/search/)

## Dataset examples

Query:
```
resource_type.type:"dataset" AND filetype:"tpr"
```

- [Simulations of a beta-2 adrenergic receptor monomer on a flat membrane](https://zenodo.org/record/4114422)
- [GROMACS simulations of unfolding of ubiqutin in a strong electric field](https://zenodo.org/record/4056037)


In zip files:

Query:
```
resource_type.type:"dataset" AND filetype:"zip" AND (simulation* "molecular dynamics" gromacs charmm namd)
```

- [All-atom molecular dynamics simulations of SARS-CoV-2 envelope protein E](https://zenodo.org/record/4743386)
- [Structural dynamics of DNA depending on methylation pattern: Simulation dataset](https://zenodo.org/record/3992686)
- [Exploring the interaction of a curcumin azobioisostere with Abeta42 dimers using replica exchange molecular dynamics simulations](https://zenodo.org/record/5573728)
- [Molecular dynamics simulation data of regulatory ACT domain dimer of human phenylalanine hydroxylase (PAH) (with unbound ligand)](https://zenodo.org/record/3814193) (with multiple zip files)


Some dataset cannot be found with keywords. Fort instance:
- [Ternary lipid composition in a curved geometry, more flat surface](https://zenodo.org/record/4644379)
# FigShare documentation

## File size

According to FigShare [FAQ](https://help.figshare.com/):

> Freely-available Figshare.com accounts have the following limits for sharing scholarly content:
storage quota: 20GB
max individual file size: 20GB
max no of collections: 100
max no of projects: 100
max no of items: 500
max no of files per item: 500
max no of collaborators on project: 100
max no of authors per item, collection: 100
max no of item version: 50
If you have more than 500 files that you need to include in an item, please create an archive (or archives) for the files (e.g. zip file).
If an individual would like to publish outputs larger than 20GB (up to many TBs), please consider Figshare+, our Figshare repository for FAIR-ly sharing big datasets that allows for more storage, larger files, additional metadata and license options, and expert support. There is a one-time cost associated with Figshare+ to cover the cost of storing the data persistently ad infinitum. Find out more about Figshare+ or get in touch at review@figshare.com with the storage amount needed and we will find the best way to support your data sharing.

> For those using an institutional version of Figshare, the number of collaboration spaces will be determined by your institution. Please contact your administrator.

So we don't expect much files to have an individual size above 20 GB.

## API

[REST API](https://docs.figshare.com/)

## Query

[Search guide](https://help.figshare.com/article/how-to-use-advanced-search-in-figshare)

## Dataset examples

### MD-related file types

Query:
```
resource_type.type:"dataset" AND filetype:"tpr"
```

Datasets:

- [Molecular dynamics of DSB in nucleosome](https://figshare.com/articles/dataset/M1_gro/5840706)
- [a-Synuclein short MD simulations:homo-A53T](https://figshare.com/articles/dataset/a-Synuclein_short_MD_simulations_homo-A53T/7007552)
- [Molecular Dynamics Protocol with Gromacs 4.0.7](https://figshare.com/articles/dataset/Molecular_Dynamics_Protocol_with_Gromacs_4_0_7/104603)


### zip files

currently not scraping


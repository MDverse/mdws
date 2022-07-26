# OSF documentation

## Direct search

Web form: https://osf.io/search/. Example with .mdp files: https://osf.io/search/?q=mdp&filter=file&page=1

Query follows [Lucene Search Query Help](https://extensions.xwiki.org/xwiki/bin/view/Extension/Search+Application+Query+Syntax)

> **Note**  
> OSF does not allow to search by file extension

## API

API to search for files: https://api.osf.io/v2/search/files/. Example with .mdp files: https://api.osf.io/v2/search/files/?q=mdp&page=1

[API](https://developer.osf.io/) documentation.

A token is required to use the API programmatically. Create one from your user [settings](https://osf.io/settings/tokens). Select the `osf.full_read` scope. Save this token in a `.env` file:
```
OSF_TOKEN=<YOUR OSF TOKEN HERE>
```

### Scraping strategy

1. [Search for relevant files](https://api.osf.io/v2/search/files/). Loop on file extensions with keywords. Results are paginated. Extract a set of unique datasets.
1. For each dataset, retrieve [informations](https://api.osf.io/v2/nodes/pdszh/) and [files list](https://api.osf.io/v2/nodes/pdszh/files/).
1. [Retrieve files informations](https://api.osf.io/v2/nodes/pdszh/files/osfstorage/). Results are paginated.


## Dataset examples

Dataset with folders:

- [ULK1 kinase domain MD simulations](https://osf.io/8xuaj/). Gromacs files with Charmm forcefield.

Dataset with components:

- [Voltage-sensing](https://osf.io/ugkwa/). Gromacs files.

Dataset with zip files: 

- [Molecular dynamics simulation of floating sphere...](https://osf.io/a3gjv/)
- [LN17351-MD model-NAMD conf.](https://osf.io/a3cpn/). Overview of the content of [`AllModel.zip`](https://osf.io/p7wmj)
- [Supplemental materials for preprint: Energetics of Interfacial Interactions...](https://osf.io/gwem8/)

Note: we cannot easily catch the content of zip files as displayed by OSF since the overview is Javascript based. See for instance the source of the page with the content of [`AllModel.zip`](https://osf.io/p7wmj). More advanced solutions such as [selenium](https://www.selenium.dev/) might be useful.

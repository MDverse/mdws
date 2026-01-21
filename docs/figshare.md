# Figshare documentation

## File size

According to Figshare [documentation](https://info.figshare.com/user-guide/file-size-limits-and-storage/):

> All figshare.com accounts are provided with 20GB of private storage and are able to upload individual files up to 20GB.

So we don't expect files to have an individual size above 20 GB.

## API

### Documentation

- [How to use the Figshare API](https://info.figshare.com/user-guide/how-to-use-the-figshare-api/)
- [API documentation](https://docs.figshare.com/)

### Token

Figshare requires a token to access its API: [How to get a personnal token](https://info.figshare.com/user-guide/how-to-get-a-personal-token/)

### URL

https://api.figshare.com/v2/

### Query

[Search guide](https://docs.figshare.com/#search)

### Rate limiting

> We do not have automatic rate limiting in place for API requests. However, we do carry out monitoring to detect and mitigate abuse and prevent the platform's resources from being overused. We recommend that clients use the API responsibly and do not make more than one request per second. We reserve the right to throttle or block requests if we detect abuse.

Source: https://docs.figshare.com/#figshare_documentation_api_description_rate_limiting

## Datasets

### Search for MD-related datasets

- Endpoint: `/articles/search`
- Documentation: <https://docs.figshare.com/#articles_search>

We seach MD-related datasets by searching for file types and keywords if necessary. Keywords are searche into `:title:`, `:description:` and `:keywords:` text fields. Example queries:

```none
resource_type.type:"dataset" AND filetype:"tpr"
```

or

```none
:extension: mdp AND (:title: 'md simulation' OR :description: 'md simulation' OR :keyword: 'md simulation')
:extension: mdp AND (:title: 'gromacs' OR :description: 'gromacs' OR :keyword: 'gromacs')
```

Example datasets:

- [Molecular dynamics of DSB in nucleosome](https://figshare.com/articles/dataset/M1_gro/5840706)
- [a-Synuclein short MD simulations:homo-A53T](https://figshare.com/articles/dataset/a-Synuclein_short_MD_simulations_homo-A53T/7007552)
- [Molecular Dynamics Protocol with Gromacs 4.0.7](https://figshare.com/articles/dataset/Molecular_Dynamics_Protocol_with_Gromacs_4_0_7/104603)

### Search strategy

We search for all file types and keywords. Results are paginated by batch of 100 datasets.

### Get metadata for a given dataset

- Endpoint: `/articles/{dataset_id}`
- Documentation: <https://docs.figshare.com/#public_article>

Example dataset "[Molecular dynamics of DSB in nucleosome](https://figshare.com/articles/dataset/M1_gro/5840706)":

- web view: <https://figshare.com/articles/dataset/M1_gro/5840706>
- API view: <https://api.figshare.com/v2/articles/5840706>

All metadata related to a given dataset is provided, as well as all files metadata.

### Zip files

Zip files content is available with a preview (similar to Zenodo). The only metadata available within this preview is the file name (no file size, no md5sum).

Example dataset "[Molecular Dynamics Simulations](https://figshare.com/articles/dataset/Molecular_Dynamics_Simulations/30307108?file=58572346)":

- The content of the file "Molecular Dynamics Simulations.zip" is available at <https://figshare.com/ndownloader/files/58572346/preview/58572346/structure.json>

We need to emulate a web browser to access the URLs linking to the contents of zip files. Otherwise, we get a 202 code.

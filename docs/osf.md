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
OSF_TOKEN=<YOUR TOKEN HERE>
```

### Scraping strategy

1. [Search for relevant files](https://api.osf.io/v2/search/files/). Loop on file extensions with keywords. Results are paginated. Extract a set of unique datasets.
1. For each dataset, retrieve [informations](https://api.osf.io/v2/nodes/pdszh/) and [files list](https://api.osf.io/v2/nodes/pdszh/files/).
1. [Retrieve files informations](https://api.osf.io/v2/nodes/pdszh/files/osfstorage/). Results are paginated.


## Data examples

Dataset with zip files: 

- https://osf.io/ncfqm/
- https://osf.io/a3gjv/


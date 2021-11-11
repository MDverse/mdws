# Molecular Dynamics web scrapper

## Setup your environment

Clone the repository:
```
git clone https://github.com/MDverse/mdws.git
```

Move to the new directory:
```
cd mdws
```

Create a [conda](https://docs.conda.io/en/latest/miniconda.html) environment:
```
conda env create -f dashboard_conda_env.yml
```

Load the `mdws` conda environment:
```
conda activate mdws
```

## Add API keys

### Zenodo

Create a token here: <https://zenodo.org/account/settings/applications/tokens/new/>  
and store it in the file `.env`:
```
ZENODO_TOKEN=YOUR-ZENODO-TOKEN
```
This file is ignored from git.

## Scrap Zenodo

```python
python scrap_zenodo.py
```
import os
from pathlib import Path

import requests
import dotenv

def read_zenodo_token():
    """Read file Zenodo token from disk."""
    dotenv.load_dotenv(".env")
    return os.environ.get("ZENODO_TOKEN", "default")


ZENODO_TOKEN = read_zenodo_token()
print(ZENODO_TOKEN)

# Basic Zenodo query
r = requests.get('https://zenodo.org/api/deposit/depositions',
                 params={'access_token':ZENODO_TOKEN})
print(f"Status code:{r.status_code}")
# Status code should be 200





def search_zenodo(page=1, hits_per_page=10, year=2016):
    response = requests.get("https://zenodo.org/api/records",
                            params={"q": ("(title:(+molecular +dynamics) OR description:(+molecular +dynamics)')"
                                         f" AND publication_date:[{year}-01-01 TO {year}-12-31]"
                                          " AND access_right:open"),
                                    "type": "dataset",
                                    "size": hits_per_page,
                                    "page": page,
                                    "status": "published",
                                    "access_token": ZENODO_TOKEN})
    return response.json()
resp_json = search_zenodo(hits_per_page=100, year=2017)
total_hits = resp_json["hits"]["total"]
print(f'Number of hits: {total_hits}')


# Record example

response = requests.get("https://zenodo.org/api/records/53887",
                        params={"access_token": ZENODO_TOKEN})
resp_json = response.json()
print(resp_json)

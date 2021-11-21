import os
from pathlib import Path

import requests
import dotenv
import pandas as pd
import argparse

def get_arg():
    """Argument parser
    Returns:
        arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('filetype', nargs='+', metavar='filetype', type=str, help="Filetype(s) of the ZENODO query.")
    return parser.parse_args()

def read_zenodo_token():
    """Read file Zenodo token from disk."""
    dotenv.load_dotenv(".env.txt")
    return os.environ.get("ZENODO_TOKEN", "default")


ZENODO_TOKEN = read_zenodo_token()
print(ZENODO_TOKEN)

# Basic Zenodo query
r = requests.get('https://zenodo.org/api/deposit/depositions',
                 params={'access_token': ZENODO_TOKEN})
print(f"Status code:{r.status_code}")


# Status code should be 200


def search_zenodo(page=1, hits_per_page=10, year=2016):
    """Makes a request on the Zenodo website.
    """
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


# resp_json = search_zenodo(hits_per_page=100, year=2017)
# total_hits = resp_json["hits"]["total"]
# print(f'Number of hits: {total_hits}')


# Record example

response = requests.get("https://zenodo.org/api/records/53887",
                        params={"access_token": ZENODO_TOKEN})
resp_json = response.json()


# print(resp_json)


# Query Zenodo

def search_zenodo_by_filetype(filetype, page=1, hits_per_page=10):
    """Search for datasets containing tpr files.
    """
    response = requests.get("https://zenodo.org/api/records",
                            params={"q": f'resource_type.type:"dataset" AND filetype:"{filetype}"',
                                    "type": "dataset",
                                    "size": hits_per_page,
                                    "page": page,
                                    "status": "published",
                                    "access_token": ZENODO_TOKEN})
    return response.json()


def extract_records(response_json):
    """Extract information from the Zenodo records.
    Arguments:
        response_json: JSON object obtained after a request on Zenodo
    Returns:
        records: list of information about datasets
        files: list of information about files
    """
    records = []
    files = []
    for hit in response_json["hits"]["hits"]:
        record = {}
        record["dataset_id"] = hit["id"]
        record["origin"] = "zenodo"
        record["doi"] = hit["doi"]
        record["title"] = hit["metadata"]["title"]
        record["date_creation"] = hit["created"]
        record["date_last_modified"] = hit["updated"]
        record["author"] = hit["metadata"]["creators"][0]["name"]
        if "keywords" in hit["metadata"]:
            record["keywords"] = ' ; '.join([str(elem) for elem in hit["metadata"]["keywords"]])
        else:
            record["keywords"] = ""
        record["file_number"] = len(hit["files"])
        record["download_number"] = hit["stats"]["version_downloads"]
        record["view_number"] = hit["stats"]["version_views"]
        record["access_right"] = hit["metadata"]["access_right"]
        if record["access_right"] != "open":
            continue
        record["license"] = hit["metadata"]["license"]["id"]
        records.append(record)
        for file_in in hit["files"]:
            file_dict = {"dataset_id": record["dataset_id"],
                         "origin": record["origin"],
                         "doi": record["doi"],
                         "title": record["title"],
                         "date_creation": record["date_creation"],
                         "date_last_modified": record["date_last_modified"],
                         "author": record["author"],
                         "keywords": record["keywords"],
                         "file_number": record["file_number"],
                         "download_number": record["download_number"],
                         "view_number": record["view_number"],
                         "license": record["license"],
                         "file_name": file_in["key"],
                         "file_extension": file_in["type"],
                         "file_size": file_in["size"],
                         "file_url": file_in["links"],
                         "file_md5": file_in["checksum"],
                         "file_type": file_in["type"]}
            files.append(file_dict)
    return records, files

args = get_arg()
max_hits_per_record = 10_000
max_hits_per_page = 100
all_records = []
all_files = []
for i in range(len(args.filetype)):
    zenodo_records = []
    zenodo_files = []
    resp_json = search_zenodo_by_filetype(hits_per_page=1, filetype=args.filetype[i])
    total_hits = resp_json["hits"]["total"]
    page_max = total_hits // max_hits_per_page + 1
    for page in range(1, page_max + 1):
        resp_json = search_zenodo_by_filetype(page=page, hits_per_page=max_hits_per_page, filetype=args.filetype[i])
        records_tmp, files_tmp = extract_records(resp_json)
        zenodo_records += records_tmp
        all_records += records_tmp
        zenodo_files += files_tmp
        all_files += files_tmp
        #print(f"year {year} -- page {page} / {page_max} ({len(records_tmp)})")
        if page * max_hits_per_page >= max_hits_per_record:
            print("Max hits per query reached!")
            break
    print(f"Number of Zenodo datasets found with files {args.filetype[i]}: {len(zenodo_records)}")
    print(f"Number of files found from all these datasets: {len(zenodo_files)}")

records_df = pd.DataFrame(all_records).set_index("dataset_id")
records_df.drop_duplicates(subset="title", keep="first", inplace=True)
#files_df = pd.DataFrame(all_files).set_index("dataset_id")
#files_df.drop_duplicates(subset="title", keep="first", inplace=True)
print(f"Number of datasets found: {records_df.shape[0]}")
#print(f"Number of files found: {files_df.shape[0]}")

#records_df = pd.DataFrame(zenodo_records).set_index("dataset_id")
#records_df.to_csv("datasets.csv")
# print(records_df.shape)

#files_df = pd.DataFrame(zenodo_files).set_index("dataset_id")
#files_df.to_csv("files.csv")
# print(files_df.shape)


#interest_df = pd.DataFrame(files_df[files_df["file_type"].isin(["tpr"])])
#interest_df.to_csv("interest.csv")
# print(interest_df.shape)

#interest_df.drop_duplicates(subset="title", keep='first', inplace=True)
#print(f"Number of datasets found with tpr files: {interest_df.shape[0]}")  # 473 datasets with tpr files

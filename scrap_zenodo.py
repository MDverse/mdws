import os
from pathlib import Path

import requests
import dotenv
import pandas as pd
import argparse
import yaml
import requests
from bs4 import BeautifulSoup


def get_arg():
    """Argument parser.

    This function parses the name of the input file.

    Returns
    ----------
    str
        Name of the input file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_file", metavar="input_file", type=str, help="Input file."
    )
    return parser.parse_args()


def read_input_file():
    """Argument parser.

    This function parses the name of the input file.

    Returns
    ----------
    md_keywords : list
        Keywords related to molecular dynamics.
    generic_keywords : list
        Generic keywords for zip archives
    file_types : dict
        Dictionary with type, engine and keywords to use
    """
    arg = get_arg()
    with open(arg.input_file, "r") as f:
        data_loaded = yaml.safe_load(f)
    md_keywords = data_loaded["md_keywords"]
    generic_keywords = data_loaded["generic_keywords"]
    file_types = data_loaded["file_types"]
    return md_keywords, generic_keywords, file_types


md_keywords, generic_keywords, file_types = read_input_file()

MD_KEYWORDS = " AND (" + " OR ".join(md_keywords) + ")"
GENERIC_KEYWORDS = " AND (" + " OR ".join(generic_keywords) + ")"


def read_zenodo_token():
    """Read file Zenodo token from disk."""
    dotenv.load_dotenv(".env")
    return os.environ.get("ZENODO_TOKEN", "default")


ZENODO_TOKEN = read_zenodo_token()
print(ZENODO_TOKEN)

# Basic Zenodo query
r = requests.get(
    "https://zenodo.org/api/deposit/depositions", params={"access_token": ZENODO_TOKEN}
)
print(f"Status code:{r.status_code}")

# Status code should be 200


"""def search_zenodo(page=1, hits_per_page=10, year=2016):
    #Makes a request on the Zenodo website.
    response = requests.get("https://zenodo.org/api/records",
                            params={"q": ("(title:(+molecular +dynamics) OR description:(+molecular +dynamics)')"
                                          f" AND publication_date:[{year}-01-01 TO {year}-12-31]"
                                          " AND access_right:open"),
                                    "type": "dataset",
                                    "size": hits_per_page,
                                    "page": page,
                                    "status": "published",
                                    "access_token": ZENODO_TOKEN})
    return response.json()"""

# resp_json = search_zenodo(hits_per_page=100, year=2017)
# total_hits = resp_json["hits"]["total"]
# print(f'Number of hits: {total_hits}')


# Record example

response = requests.get(
    "https://zenodo.org/api/records/53887", params={"access_token": ZENODO_TOKEN}
)
resp_json = response.json()


# print(resp_json)


# Query Zenodo


def search_zenodo_by_filetype(filetype, page=1, hits_per_page=10):
    """Search for datasets containing tpr files."""
    response = requests.get(
        "https://zenodo.org/api/records",
        params={
            "q": f'resource_type.type:"dataset" AND filetype:"{filetype}"',
            "type": "dataset",
            "size": hits_per_page,
            "page": page,
            "status": "published",
            "access_token": ZENODO_TOKEN,
        },
    )
    return response.json()


def search_zenodo_with_query(query, page=1, hits_per_page=10):
    """Search for datasets."""
    response = requests.get(
        "https://zenodo.org/api/records",
        params={
            "q": query,
            "size": hits_per_page,
            "page": page,
            "status": "published",
            "access_token": ZENODO_TOKEN,
        },
    )
    return response.json()


def scrap_zip_content(files_df):
    zip = []
    for i in range(files_df.shape[0]):
        if (files_df["file_type"].iloc[i]) == "zip":
            URL = f"https://zenodo.org/record/{files_df.iloc[i]['dataset_id']}/preview/{files_df.iloc[i]['file_name']}"
            print(URL)
            r = requests.get(URL)
            soup = BeautifulSoup(r.content, "html5lib")
            table = soup.find("ul", attrs={"class": "tree list-unstyled"})
            chain = []
            for row in table.findAll("span"):
                chain.append(row.text)
            for j in range(0, len(chain), 2):
                size = chain[j + 1].split()
                if size[1] == "GB":
                    s = float(size[0]) * (10 ** 9)
                elif size[1] == "MB":
                    s = float(size[0]) * (10 ** 6)
                elif size[1] == "kB":
                    s = float(size[0]) * (10 ** 3)
                else:
                    s = float(size[0])
                file_dict = {
                    "dataset_id": files_df.iloc[i]["dataset_id"],
                    "origin": files_df.iloc[i]["origin"],
                    "doi": files_df.iloc[i]["doi"],
                    "title": files_df.iloc[i]["title"],
                    "date_creation": files_df.iloc[i]["date_creation"],
                    "date_last_modified": files_df.iloc[i]["date_last_modified"],
                    "author": files_df.iloc[i]["author"],
                    "keywords": files_df.iloc[i]["keywords"],
                    "file_number": files_df.iloc[i]["file_number"],
                    "download_number": files_df.iloc[i]["download_number"],
                    "view_number": files_df.iloc[i]["view_number"],
                    "license": files_df.iloc[i]["license"],
                    "from_zip_file": files_df.iloc[i]["file_name"],
                    "file_name": chain[j],
                    "file_extension": chain[j][-3:],
                    "file_size": s,
                    "file_url": "",
                    "file_md5": "",
                    "file_type": chain[j][-3:],
                }
                zip.append(file_dict)
    zip_df = pd.DataFrame(zip).set_index("dataset_id")
    return zip_df


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
            record["keywords"] = " ; ".join(
                [str(elem) for elem in hit["metadata"]["keywords"]]
            )
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
            file_dict = {
                "dataset_id": record["dataset_id"],
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
                "from_zip_file": "",
                "file_name": file_in["key"],
                "file_extension": file_in["type"],
                "file_size": file_in["size"],
                "file_url": file_in["links"],
                "file_md5": file_in["checksum"],
                "file_type": file_in["type"],
            }
            files.append(file_dict)
    return records, files


args = get_arg()
max_hits_per_record = 20_000
max_hits_per_page = 100
# all_records = []
# all_files = []
datasets_df = pd.DataFrame()
files_df = pd.DataFrame()
for i in range(len(file_types)):
    zenodo_records = []
    zenodo_files = []
    query = f'resource_type.type:"dataset" AND filetype:"{file_types[i]["type"]}"'
    if file_types[i]["keywords"] == "md_keywords":
        query += MD_KEYWORDS
    elif file_types[i]["keywords"] == "generic_keywords":
        query += GENERIC_KEYWORDS
    print(query)
    resp_json = search_zenodo_with_query(query, hits_per_page=1)
    total_hits = resp_json["hits"]["total"]
    page_max = total_hits // max_hits_per_page + 1
    for page in range(1, page_max + 1):
        resp_json = search_zenodo_with_query(
            query, page=page, hits_per_page=max_hits_per_page
        )
        # print(resp_json)
        records_tmp, files_tmp = extract_records(resp_json)
        zenodo_records += records_tmp
        # all_records += records_tmp
        zenodo_files += files_tmp
        # all_files += files_tmp
        datasets_df_inter = pd.DataFrame(records_tmp).set_index(
            "dataset_id", drop=False
        )
        datasets_df = pd.concat([datasets_df, datasets_df_inter], ignore_index=True)
        datasets_df.drop_duplicates(keep="first", inplace=True)
        datasets_df.to_csv("datasets.csv")
        files_df_inter = pd.DataFrame(files_tmp).set_index("dataset_id", drop=False)
        files_df = pd.concat([files_df, files_df_inter], ignore_index=True)
        files_df.drop_duplicates(
            subset=["doi", "file_name"], keep="first", inplace=True
        )
        # print(f"year {year} -- page {page} / {page_max} ({len(records_tmp)})")
        if page * max_hits_per_page >= max_hits_per_record:
            print("Max hits per query reached!")
            break
    print(
        f"Number of Zenodo datasets found with files {file_types[i]['type']}: {len(zenodo_records)}"
    )
    print(f"Number of files found from all these datasets: {len(zenodo_files)}")

print(f"Number of datasets found: {datasets_df.shape[0]}")
datasets_df.to_csv("datasets.csv")

files_df.to_csv("files.csv")
zip_df = scrap_zip_content(files_df)
files_df = pd.concat([files_df, zip_df], ignore_index=True)


print(f"Number of files found: {files_df.shape[0]}")


files_df.to_csv("files.csv")

# records_df = pd.DataFrame(zenodo_records).set_index("dataset_id")
# records_df.to_csv("datasets.csv")
# print(records_df.shape)

# files_df = pd.DataFrame(zenodo_files).set_index("dataset_id")
# files_df.to_csv("files.csv")
# print(files_df.shape)


# interest_df = pd.DataFrame(files_df[files_df["file_type"].isin(["tpr"])])
# interest_df.to_csv("interest.csv")
# print(interest_df.shape)

# interest_df.drop_duplicates(subset="title", keep='first', inplace=True)
# print(f"Number of datasets found with tpr files: {interest_df.shape[0]}")  # 473 datasets with tpr files

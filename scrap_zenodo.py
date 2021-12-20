"""Scrap molecular dynamics datasets and files from Zenodo."""

import argparse
from datetime import datetime
from pathlib import Path
import os


from bs4 import BeautifulSoup
import dotenv
import pandas as pd
import requests
import yaml


def get_cli_arguments():
    """Argument parser.

    This function parses the name of the yaml input file.

    Returns
    ----------
    str
        Name of the yaml input file.
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
    -------
    file_types : dict
        Dictionary with type, engine and keywords to use
    md_keywords : list
        Keywords related to molecular dynamics.
    generic_keywords : list
        Generic keywords for zip archives
    """
    arg = get_cli_arguments()
    with open(arg.input_file, "r") as param_file:
        print(f"Reading parameters from: {arg.input_file}")
        data_loaded = yaml.safe_load(param_file)
    md_keywords = data_loaded["md_keywords"]
    generic_keywords = data_loaded["generic_keywords"]
    file_types = data_loaded["file_types"]
    return file_types, md_keywords, generic_keywords


def extract_date(date_str):
    """Extract and format date from a string.

    Parameters
    ----------
    date_str : str
        Date as a string in ISO 8601.
        For example: 2020-07-29T19:22:57.752335+00:00

    Returns
    -------
    str
        Date as in string in YYYY-MM-DD format.
        For example: 2020-07-29
    """
    date = datetime.fromisoformat(date_str)
    return f"{date:%Y-%m-%d}"


def read_zenodo_token():
    """Read file Zenodo token from disk."""
    dotenv.load_dotenv(".env")
    return os.environ.get("ZENODO_TOKEN", "default")


def test_zenodo_connection(token):
    """Test connection to Zenodo API.

    Parameters
    ----------
    token : str
        Token for Zenodo API
    """
    print("Trying connection to Zenodo...")
    # Basic Zenodo query
    r = requests.get(
        "https://zenodo.org/api/deposit/depositions",
        params={"access_token": token}
    )
    # Status code should be 200
    print(f"Status code: {r.status_code}", end="")
    if r.status_code == 200:
        print(" success!")
    else:
        print(" failed!")


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
    """Search for datasets.

    Arguments
    ----------
    query: str
        Query.
    page: int
        Number of page.
    hits_per_page: int
        Number of hits per pages.
    Returns
    ----------
    response.json(): dict
        JSON object obtained after a request on Zenodo.
    """
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
    """Scrap information from files contained in zip archives.

    Arguments
    ----------
    files_df: dataframe
        Dataframe with information about files.
    Returns
    ----------
    zip_df: dataframe
        Dataframe with information about files in zip archive.
    """
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

    Arguments
    ----------
    response_json: dict
        JSON object obtained after a request on Zenodo API.
    Returns
    ----------
    records: list
        List of dictionnaries. Information on datasets.
    files: list
        List of dictionnaies. Information on files.
    """
    records = []
    files = []
    if response_json["hits"]["hits"]:
        for hit in response_json["hits"]["hits"]:
            record_dict = {}
            record_dict["dataset_id"] = hit["id"]
            record_dict["origin"] = "zenodo"
            record_dict["doi"] = hit["doi"]
            record_dict["date_creation"] = extract_date(hit["created"])
            record_dict["date_last_modified"] = extract_date(hit["updated"])
            record_dict["date_fetched"] = datetime.now().isoformat(timespec="seconds")
            record_dict["file_number"] = len(hit["files"])
            record_dict["download_number"] = hit["stats"]["version_downloads"]
            record_dict["view_number"] = hit["stats"]["version_views"]
            record_dict["access_right"] = hit["metadata"]["access_right"]
            if record_dict["access_right"] != "open":
                continue
            record_dict["license"] = hit["metadata"]["license"]["id"]
            record_dict["title"] = hit["metadata"]["title"]
            record_dict["author"] = hit["metadata"]["creators"][0]["name"]
            record_dict["keywords"] = ""
            if "keywords" in hit["metadata"]:
                record_dict["keywords"] = " ; ".join(
                    [str(elem) for elem in hit["metadata"]["keywords"]]
                )
            records.append(record_dict)
            for file_in in hit["files"]:
                file_dict = {
                    "dataset_id": record_dict["dataset_id"],
                    "origin": record_dict["origin"],
                    "file_type": file_in["type"],
                    "file_size": file_in["size"],
                    "file_md5": file_in["checksum"].replace("md5:", ""),
                    "from_zip_file": False,
                    "file_name": file_in["key"],
                    "file_url": file_in["links"]["self"],
                    "origin_zip_file": "None"
                }
                files.append(file_dict)
    return records, files


if __name__ == "__main__":
    # Read Zenodo token
    ZENODO_TOKEN = read_zenodo_token()
    test_zenodo_connection(ZENODO_TOKEN)
    # Read parameter file
    FILE_TYPES, MD_KEYWORDS, GENERIC_KEYWORDS = read_input_file()
    QUERY_MD_KEYWORDS = " AND (" + " OR ".join(MD_KEYWORDS) + ")"
    QUERY_GENERIC_KEYWORDS = " AND (" + " OR ".join(GENERIC_KEYWORDS) + ")"

    max_hits_per_record = 10_000
    max_hits_per_page = 100

    datasets_df = pd.DataFrame()
    files_df = pd.DataFrame()
    for file_type in FILE_TYPES:
        print(f"Looking for filetype: {file_type['type']}")
        query_records = []
        query_files = []
        query = (f'resource_type.type:"dataset" '
                 f'AND filetype:"{file_type["type"]}"')
        if file_type["keywords"] == "md_keywords":
            query += QUERY_MD_KEYWORDS
        elif file_type["keywords"] == "generic_keywords":
            query += QUERY_GENERIC_KEYWORDS
        print(f"Query:\n{query}")
        resp_json = search_zenodo_with_query(query, hits_per_page=1)
        total_hits = resp_json["hits"]["total"]
        page_max = total_hits // max_hits_per_page + 1
        for page in range(1, page_max + 1):
            print(f"Page: {page}")
            resp_json = search_zenodo_with_query(
                query, page=page, hits_per_page=max_hits_per_page
            )
            datasets_tmp, files_tmp = extract_records(resp_json)
            datasets_df_tmp = pd.DataFrame(datasets_tmp).set_index(
                "dataset_id", drop=False
            )
            datasets_df = pd.concat([datasets_df, datasets_df_tmp], ignore_index=True)
            datasets_df.drop_duplicates(keep="first", inplace=True)
            files_df_tmp = pd.DataFrame(files_tmp).set_index("dataset_id", drop=False)
            files_df = pd.concat([files_df, files_df_tmp], ignore_index=True)
            files_df.drop_duplicates(
                subset=["dataset_id", "file_name"], keep="first", inplace=True
            )
            if page * max_hits_per_page >= max_hits_per_record:
                print("Max hits per query reached!")
                break
        print(f"Number of datasets found: {len(datasets_tmp)}")
        print(f"Number of files found: {len(files_tmp)}")
        print("-"*20)

    print(f"Total number of datasets found: {datasets_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    # Save dataframes to disk
    datasets_df.to_csv("datasets.tsv", sep="\t", index=None)
    files_df.to_csv("files.tsv", sep="\t", index=None)
    exit(0)
    zip_df = scrap_zip_content(files_df)
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    print(f"Number of files found: {files_df.shape[0]}")
    files_df.to_csv("files.tsv", sep="\t", index=None)

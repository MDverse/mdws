"""Scrap molecular dynamics datasets and files from Zenodo."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import time


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


def normalize_file_size(file_str):
    """Normalize file size in kB.

    Parameters
    ----------
    file_str : str
        File size with unit.
        For instance: 1.8 GB, 108.7 kB

    Returns
    -------
    float
        File size in kB.
    """
    size, unit = file_str.split()
    if unit == "GB":
        size_in_kb = float(size) * 1_000_000
    elif unit == "MB":
        size_in_kb = float(size) * 1_000
    elif unit == "kB":
        size_in_kb = float(size) 
    elif unit == "Bytes":
        size_in_kb = float(size) / 1_000
    else:
        size_in_kb = 0.0
    return size_in_kb


def extract_data_from_zip_file_preview(url, token):
    """Extract data from zip file preview.

    Parameters
    ----------
    url : str
        URL of zip file preview
    token : str
        Token for Zenodo API


    Returns
    -------
    list
        List of dictionnaries with data extracted from zip preview.
    """
    response = requests.get(url,
                            params={"access_token": token})
    
    if response.status_code != 200:
        print(f"Status code: {response.status_code}")
        print(response.headers)
    soup = BeautifulSoup(response.content, "html5lib")
    if "Zipfile is not previewable" in response.text:
        print(f"No preview available for {url}!")
        return []
    table = soup.find("ul", attrs={"class": "tree list-unstyled"})
    file_info = []
    for row in table.findAll("span"):
        file_info.append(row.text)
    file_lst = []
    for idx in range(0, len(file_info), 2):
        file_name = file_info[idx].strip()
        file_size_raw = file_info[idx+1].strip()
        file_size_in_kb = normalize_file_size(file_size_raw)
        file_dict = {
            "file_name": file_name,
            "file_size": file_size_in_kb
        }
        file_dict["file_type"] = ""
        if "." in file_name:
            file_dict["file_type"] = file_name.split(".")[-1]
        # Ignore files starting with a dot
        if file_name.startswith("."):
            continue
        file_lst.append(file_dict)
    return file_lst


def read_zenodo_token():
    """Read file Zenodo token from disk.
    
    Returns
    -------
    str
        Zenodo token.
    """
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
        print(r.headers)


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
    zip_lst = []
    zip_counter = 0
    target_df = files_df[files_df["file_type"] == "zip"]
    print(f"Number of zip files to scrap content from: {target_df.shape[0]}")
    for idx in target_df.index:
        zip_counter += 1
        if zip_counter % 60 == 0:
            time.sleep(60)
            print("---")
        URL = (f"https://zenodo.org/record/{target_df.loc[idx,'dataset_id']}"
               f"/preview/{target_df.loc[idx, 'file_name']}"
               )
        print(zip_counter, URL)
        file_lst = extract_data_from_zip_file_preview(URL, ZENODO_TOKEN)
        if file_lst == []:
            continue
        # Add common extra fields
        for file_idx in range(len(file_lst)):
            file_lst[file_idx]["dataset_id"] = target_df.loc[idx, "dataset_id"]
            file_lst[file_idx]["origin"] = target_df.loc[idx, "origin"]
            file_lst[file_idx]["from_zip_file"] = True
            file_lst[file_idx]["origin_zip_file"] = target_df.loc[idx, "file_name"]
            file_lst[file_idx]["file_url"] = ""
            file_lst[file_idx]["file_md5"] = ""
        zip_lst += file_lst
    zip_df = pd.DataFrame(zip_lst)
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
            record_dict["dataset_id"] = str(hit["id"])
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
            # Dataset description might be interesting. Not saved yet.
            # record_dict["description"] = hit["metadata"]["description"]
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
    #exit(0)
    zip_df = scrap_zip_content(files_df)
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    print(f"Number of files found: {files_df.shape[0]}")
    files_df.to_csv("files.tsv", sep="\t", index=None)

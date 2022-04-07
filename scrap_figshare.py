"""Scrap molecular dynamics datasets and files from FigShare."""
# Standard library imports
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time

# Third party imports
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
import yaml


def get_cli_arguments():
    """Argument parser.

    This function parses the name of the yaml input file.

    Returns
    -------
    str
        Name of the yaml input file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_file", metavar="input_file", type=str, help="Input file."
    )
    return parser.parse_args()


def read_input_file(arg):
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


def extract_files_from_response(json_dic, file_list):
    """Go recursively through the json directory tree structure

    Parameters
    ----------
    json_dic : dict
        json dictionary of zip file preview
    
    file_list : list
        list with filenames

    Returns
    -------
    list
        List of filenames extracted from zip preview.
    """
    for value in json_dic['files']:
        file_list.append(value['path'])
    for dir_list in json_dic['dirs']:
        file_list = extract_files_from_response(dir_list, file_list)
    return file_list


def extract_data_from_figshare_zip_file(url):
    """Extract data from zip file preview.

    Parameters
    ----------
    url : str
        URL of zip file preview

    Returns
    -------
    list
        List of dictionnaries with data extracted from zip preview.
    """
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Status code: {response.status_code}")
        print(response.headers)
        print(url)
        return {}
    file_list_json = response.json()
    file_list = extract_files_from_response(file_list_json, [])
    file_lst = []
    for idx, file in enumerate(file_list):
        file_name = file.strip()
        file_size = np.nan
        file_dict = {"file_name": file_name, "file_size": np.nan}
        file_dict["file_type"] = "none"
        if "." in file_name:
            file_dict["file_type"] = file_name.split(".")[-1].lower()
        # Ignore files starting with a dot
        if file_name.startswith("."):
            continue
        file_lst.append(file_dict)
    return file_lst


def search_figshare_with_query(query, page=1, hits_per_page=1000):
    """Search for datasets.

    Arguments
    ---------
    query: str
        Query.
    page: int
        Page number.
    hits_per_page: int
        Number of hits per pages.

    Returns
    -------
    dict
        Figshare response as a JSON object.
    """
    HEADERS = {'content-type': 'application/json'}
    response = requests.post(
        "https://api.figshare.com/v2/articles/search",
        data=f'\u007b"search_for": "{query}", "page_size":{hits_per_page}, "item_type":3, "page":{page}\u007d',
        headers=HEADERS
    )
    if response.status_code == 200:
        return response.json()
    else:
        return None


def request_figshare_dataset_with_id(datasetID):
    """Search for articles.

    Arguments
    ---------
    datasetID: str
        Dataset ID.

    Returns
    -------
    dict
        FigShare response as a JSON object.
    """
    response = requests.get(
        f"https://api.figshare.com/v2/articles/{datasetID}"
    )
    return json.loads(response.content)


def request_figshare_downloadstats_with_id(datasetID):
    """Get download stats for articles.

    Arguments
    ---------
    datasetID: str
        Dataset ID.

    Returns
    -------
    dict
        FigShare response as a JSON object.
    """
    response = requests.get(
        f"https://stats.figshare.com/total/downloads/article/{datasetID}"
    )
    return json.loads(response.content)


def request_figshare_viewstats_with_id(datasetID):
    """Get view stats for articles.

    Arguments
    ---------
    datasetID: str
        Dataset ID.

    Returns
    -------
    dict
        FigShare response as a JSON object.
    """
    response = requests.get(
        f"https://stats.figshare.com/total/views/article/{datasetID}"
    )
    return json.loads(response.content)


def scrap_figshare_zip_content(files_df):
    """Scrap information from files contained in zip archives.

    Uncertain how many files can be fetched from the preview.

    Arguments
    ---------
    files_df: dataframe
        Dataframe with information about files.

    Returns
    -------
    zip_df: dataframe
        Dataframe with information about files in zip archive.
    """
    files_in_zip_lst = []
    zip_counter = 0
    zip_files_df = files_df[files_df["file_type"] == "zip"]
    print(
        "Number of zip files to scrap content from: "
        f"{zip_files_df.shape[0]}"
    )
    for zip_idx in zip_files_df.index:
        zip_file = zip_files_df.loc[zip_idx]
        file_id = zip_file['file_url'].split('/')[-1]
        zip_counter += 1
        # According to Figshare support
        # One can run 100 requests per 5 minutes.
        # To be careful, we wait 300 secondes every 60 requests.
        sleep_time = 300
        if zip_counter % 60 == 0:
            print(
                f"Scraped {zip_counter} zip files / "
                f"{zip_files_df.shape[0]}\n"
                f"Waiting for {sleep_time} seconds..."
            )
            time.sleep(sleep_time)
        URL = (
            f"https://figshare.com/ndownloader/files/{file_id}"
            f"/preview/{file_id}/structure.json"
        )
        files_tmp = extract_data_from_figshare_zip_file(URL)
        if files_tmp == []:
            continue
        # Add common extra fields
        for idx in range(len(files_tmp)):
            files_tmp[idx]["dataset_id"] = zip_file["dataset_id"]
            files_tmp[idx]["origin"] = zip_file.loc["origin"]
            files_tmp[idx]["from_zip_file"] = True
            files_tmp[idx]["origin_zip_file"] = zip_file.loc["file_name"]
            files_tmp[idx]["file_url"] = ""
            files_tmp[idx]["file_md5"] = ""
        files_in_zip_lst += files_tmp
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    return files_in_zip_df


def decoder(string):
    """Decodes from html and removes breaks

    Arguments
    ---------
    string: str
        input string

    Returns
    -------
    str
        decoded string.
    """
    text_decode = BeautifulSoup(string, features="lxml")
    text_decode = u''.join(text_decode.findAll(text=True))
    text_decode = re.sub(r"[-\+\n\r]", " ", text_decode)
    return text_decode


def extract_records(hit, fetch_description=False):
    """Extract information from the FigShare records.

    Arguments
    ---------
    response_json: dict
        JSON object obtained after a request on FigShare API.

    Returns
    -------
    records: list
        List of dictionnaries. Information on datasets.
    files: list
        List of dictionnaies. Information on files.
    """
    records = []
    files = []
    if hit["is_embargoed"] != False:
        return records, files
    record_dict = {
        "dataset_id": str(hit["id"]),
        "origin": "figshare",
        "doi": hit["doi"],
        "date_creation": extract_date(hit["created_date"][:-1]),
        "date_last_modified": extract_date(hit["modified_date"][:-1]),
        "date_fetched": datetime.now().isoformat(timespec="seconds"),
        "file_number": len(hit["files"]),
        "download_number": request_figshare_downloadstats_with_id(hit['id'])["totals"],
        "view_number": request_figshare_viewstats_with_id(hit['id'])["totals"],
        "license": hit["license"]["name"],
        "title": decoder(hit["title"]),
        "author": hit["authors"][0]["full_name"],
        "keywords": "",
        "dataset_url": hit["url"],
    }
    if "tags" in hit:
        record_dict["keywords"] = " ; ".join(
            [str(decoder(keyword)) for keyword in hit["tags"]]
        )
    if fetch_description:
        # Dataset description might be interesting. Not saved yet.
        record_dict["description"] = decoder(hit["description"])
    records.append(record_dict)
    for file_in in hit["files"]:
        if len(file_in["name"].split('.'))==1:
            filetype = "none"
        else:
            filetype = file_in["name"].split('.')[-1].lower()
        file_dict = {
            "dataset_id": record_dict["dataset_id"],
            "origin": record_dict["origin"],
            "file_type": filetype,
            "file_size": file_in["size"],
            "file_md5": file_in["computed_md5"],
            "from_zip_file": False,
            "file_name": file_in["name"],
            "file_url": file_in["download_url"],
            "origin_zip_file": "None",
        }
        files.append(file_dict)
    return records, files


def main_scrap_figshare(arg, scrap_zip=False, fetch_description=False):
    """
    Main function called as default at the end.
    """
    # Read parameter file
    FILE_TYPES, MD_KEYWORDS, GENERIC_KEYWORDS = read_input_file(arg)
    # Query with keywords are build in the loop as Figshare has a char limit

    # The best strategy is to use paging.
    MAX_HITS_PER_PAGE = 1000

    datasets_df = pd.DataFrame()
    files_df = pd.DataFrame()
    prev_datasets_count = 0
    prev_file_count = 0
    for file_type in FILE_TYPES:
        print(f"Looking for filetype: {file_type['type']}")
        query_records = []
        query_files = []
        base_query = (
            f':extension: {file_type["type"]}'
        )
        if file_type["keywords"] == "md_keywords":
            add_keywords = len(MD_KEYWORDS)
            print(f"Additional keywords for query: {', '.join(MD_KEYWORDS)}")
        elif file_type["keywords"] == "generic_keywords":
            add_keywords = len(GENERIC_KEYWORDS)
            print(f"Additional keywords for query: {', '.join(GENERIC_KEYWORDS)}")
        else:
            add_keywords = 1
        # Go through all keywords as query length for FigShare is limited
        for keywordID in range(0,add_keywords):
            if file_type["keywords"] == "md_keywords":
                query = (
                    f"{base_query} AND (:title: '{MD_KEYWORDS[keywordID]}' "
                    f"OR :description: '{MD_KEYWORDS[keywordID]}' OR :keyword: '{MD_KEYWORDS[keywordID]}')"
                )
            elif file_type["keywords"] == "generic_keywords":
                query = (
                    f"{base_query} AND (:title: '{GENERIC_KEYWORDS[keywordID]}' "
                    f"OR :description: '{GENERIC_KEYWORDS[keywordID]}' OR :keyword: '{GENERIC_KEYWORDS[keywordID]}')"
                )
            else:
                query = base_query
            # print(f"Query:\n{query}")
            # First get the total number of hits for a given query.
            resp_json = search_figshare_with_query(
               query, hits_per_page=1
            )
            # Then, slice the query by page.
            page=1
            while page!=0:
                # print(f"Page: {page}")
                resp_json = search_figshare_with_query(
                    query, page=page, hits_per_page=MAX_HITS_PER_PAGE
                )
                if len(resp_json)==0:
                    # print("Max hits per query reached!")
                    page = 0
                else:
                    page+=1
                    # Go through all datasets
                    # print(f"Number of datasets: {len(resp_json)}")
                    resp_json = [json.loads(i) for i in set([json.dumps(i) for i in [dict(sorted(i.items())) for i in resp_json]])]
                    for dataset in resp_json:
                        dataset_id = dataset['id']
                        if datasets_df.empty or not dataset_id in datasets_df['dataset_id']:
                            resp_json_article = request_figshare_dataset_with_id(dataset_id)
                            datasets_tmp, files_tmp = extract_records(resp_json_article, fetch_description=fetch_description)
                            # Merge datasets
                            datasets_df_tmp = pd.DataFrame(datasets_tmp)
                            datasets_df = pd.concat(
                                [datasets_df, datasets_df_tmp], ignore_index=True
                            )
                            datasets_df.drop_duplicates(
                                subset=["dataset_id", "origin"], keep="first", inplace=True)
                            # Merge files
                            files_df_tmp = pd.DataFrame(files_tmp)
                            files_df = pd.concat([files_df, files_df_tmp], ignore_index=True)
                            files_df.drop_duplicates(
                                subset=["dataset_id", "file_name", "file_md5"], keep="first", inplace=True
                            )

        print(f"Number of datasets found: {len(datasets_df)-prev_datasets_count}")
        print(f"Number of files found: {len(files_df)-prev_file_count}")
        print("-" * 30)
        prev_datasets_count = len(datasets_df)
        prev_file_count = len(files_df)


    print(f"Total number of datasets found: {datasets_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    # Save dataframes to disk
    datasets_df.to_csv("figshare_datasets.tsv", sep="\t", index=False)
    files_df.to_csv("figshare_files.tsv", sep="\t", index=False)

    if scrap_zip:
        # Scrap zip files content
        zip_df = scrap_figshare_zip_content(files_df)
        # We don't remove duplicates here because
        # one zip file can contain several files with the same name
        # but within different folders.
        files_df = pd.concat([files_df, zip_df], ignore_index=True)
        print(f"Number of files found inside zip files: {zip_df.shape[0]}")
        print(f"Total number of files found: {files_df.shape[0]}")
        files_df.to_csv("figshare_files.tsv", sep="\t", index=False)    


    return datasets_df, files_df


if __name__ == "__main__":
    # Parse input arguments
    arg = get_cli_arguments()
    
    # Call extract main scrap function
    main_scrap_figshare(arg, scrap_zip=True, fetch_description=False)


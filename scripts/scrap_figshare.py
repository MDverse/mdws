"""Scrap molecular dynamics datasets and files from FigShare."""

from datetime import datetime
import logging
import json
import pathlib
import time
import os
import sys
import numpy as np
import pandas as pd
import requests
import httpx

import toolbox
from logger import create_logger
from figshare_api import FigshareAPI




# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info

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
    """Walk recursively through the json directory tree structure.

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
    for value in json_dic["files"]:
        file_list.append(value["path"])
    for dir_list in json_dic["dirs"]:
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
    # We need to use a classical web browser user agent to avoid a 403 error.
    headers = {
        "content-type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }
    # 'allow_redirects=True' is necessary to follow the redirection of the zip preview,
    # generally to S3 AWS servers.
    response = requests.get(
        url,
        headers=headers,
        allow_redirects=True
    )

    if response.status_code != 200:
        print(f"Status code: {response.status_code}")
        print(response.headers)
        print(url)
        return []

    file_names = extract_files_from_response(response.json(), [])
    file_list = []
    for name in file_names:
        file_name = name.strip()
        file_dict = {
            "file_name": file_name,
            "file_size": np.nan,
            "file_type": toolbox.extract_file_extension(file_name),
        }
        file_list.append(file_dict)

    return file_list


def search_figshare_with_query(query: str, page: int = 1, hits_per_page: int = 1000) -> dict:
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
    headers = {"content-type": "application/json"}
    data = {
        "order": "published_date",
        "search_for": f"{query}",
        "page": page,
        "page_size": hits_per_page,
        "item_type": 3,  # datasets
        "order_direction": "desc"
    }
    #f'\u007b"search_for": "{query}", "page_size":{hits_per_page}, "item_type":3, "page":{page}\u007d',
    response = httpx.post(
        url="https://api.figshare.com/v2/articles/search",
        data=data,
        headers=headers,
    )
    print(response.status_code, response.headers, response.content, sep="\n\n")
    sys.exit(1)
    if response.status_code == 200:
        return response.json()
    else:
        log.warning(f"Status code is {response.status_code}")
        return {}


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
    response = requests.get(f"https://api.figshare.com/v2/articles/{datasetID}")
    if response.status_code == 200:
        return response.json()
    else:
        return None
    # return json.loads(response.content)


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
    files_df: Pandas dataframe
        Dataframe with information about files.

    Returns
    -------
    zip_df: Pandas dataframe
        Dataframe with information about files found in zip archive.
    """
    files_in_zip_lst = []
    zip_counter = 0
    zip_files_df = files_df[files_df["file_type"] == "zip"]
    print("Number of zip files to scrap: " f"{zip_files_df.shape[0]}")
    for zip_idx in zip_files_df.index:
        zip_file = zip_files_df.loc[zip_idx]
        file_id = zip_file["file_url"].split("/")[-1]
        zip_counter += 1
        # We cannot use the Figshare API to get the content of a zip file.
        # According to Figshare support
        # One can run 100 requests per 5 minutes (300 secondes).
        # To be careful, we wait 310 secondes every 100 requests.
        SLEEP_TIME = 310
        if zip_counter % 100 == 0:
            print(
                f"Scraped {zip_counter} zip files "
                f"({zip_files_df.shape[0] - zip_counter} remaining)"
            )
            print(f"Waiting for {SLEEP_TIME} seconds...")
            time.sleep(SLEEP_TIME)
        URL = (
            f"https://figshare.com/ndownloader/files/{file_id}"
            f"/preview/{file_id}/structure.json"
        )
        files_tmp = extract_data_from_figshare_zip_file(URL)
        if files_tmp == []:
            continue
        # Add common extra fields
        for idx in range(len(files_tmp)):
            files_tmp[idx]["dataset_origin"] = zip_file["dataset_origin"]
            files_tmp[idx]["dataset_id"] = zip_file["dataset_id"]
            files_tmp[idx]["from_zip_file"] = True
            files_tmp[idx]["origin_zip_file"] = zip_file["file_name"]
            files_tmp[idx]["file_url"] = ""
            files_tmp[idx]["file_md5"] = ""
        files_in_zip_lst += files_tmp
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    return files_in_zip_df


def extract_records(hit):
    """Extract information from the FigShare records.

    Example of record:
    https://api.figshare.com/v2/articles/5840706
    that corresponds to dataset:
    https://figshare.com/articles/dataset/M1_gro/5840706

    Arguments
    ---------
    response_json: dict
        JSON object obtained after a request on FigShare API.

    Returns
    -------
    records: list
        List of dictionnaries. Information on datasets.
    texts: list
        List of dictionnaries. Textual information on datasets
    files: list
        List of dictionnaies. Information on files.
    """
    datasets = []
    texts = []
    files = []
    if hit["is_embargoed"] != False:
        return datasets, texts, files
    dataset_dict = {
        "dataset_origin": "figshare",
        "dataset_id": str(hit["id"]),
        "doi": hit["doi"],
        "date_creation": extract_date(hit["created_date"][:-1]),
        "date_last_modified": extract_date(hit["modified_date"][:-1]),
        "date_fetched": datetime.now().isoformat(timespec="seconds"),
        "file_number": len(hit["files"]),
        "download_number": request_figshare_downloadstats_with_id(hit["id"])["totals"],
        "view_number": request_figshare_viewstats_with_id(hit["id"])["totals"],
        "license": hit["license"]["name"],
        "dataset_url": hit["url_public_html"],
    }
    datasets.append(dataset_dict)
    text_dict = {
        "dataset_origin": dataset_dict["dataset_origin"],
        "dataset_id": dataset_dict["dataset_id"],
        "title": toolbox.clean_text(hit["title"]),
        "author": toolbox.clean_text(hit["authors"][0]["full_name"]),
        "keywords": "",
        "description": toolbox.clean_text(hit["description"]),
    }
    if "tags" in hit:
        text_dict["keywords"] = ";".join(
            [toolbox.clean_text(keyword) for keyword in hit["tags"]]
        )
    texts.append(text_dict)
    for file_in in hit["files"]:
        if len(file_in["name"].split(".")) == 1:
            filetype = "none"
        else:
            filetype = file_in["name"].split(".")[-1].lower()
        file_dict = {
            "dataset_origin": dataset_dict["dataset_origin"],
            "dataset_id": dataset_dict["dataset_id"],
            "file_type": filetype,
            "file_size": file_in["size"],
            "file_md5": file_in["computed_md5"],
            "from_zip_file": False,
            "file_name": file_in["name"],
            "file_url": file_in["download_url"],
            "origin_zip_file": "None",
        }
        files.append(file_dict)
    return datasets, texts, files


def main_scrap_figshare(api: FigshareAPI, arg, scrap_zip:bool=False):
    """Scrap Figshare."""
    # Read parameter file
    FILE_TYPES, KEYWORDS, EXCLUDED_FILES, EXCLUDED_PATHS = toolbox.read_query_file(
        arg.query
    )
    # Query with keywords are build in the loop as Figshare has a character limit in query.

    # We use paging to fetch all results.
    # we query MAX_HITS_PER_PAGE hits per page.
    MAX_HITS_PER_PAGE = 100

    datasets_df = pd.DataFrame()
    texts_df = pd.DataFrame()
    files_df = pd.DataFrame()
    log.info("-" * 30)
    for file_type in FILE_TYPES:
        log.info(f"Looking for filetype: {file_type['type']}")
        base_query = f":extension: {file_type['type']}"
        target_keywords = [""]
        if file_type["keywords"] == "keywords":
            target_keywords = KEYWORDS
        # Go through all keywords as query length for FigShare is limited
        found_datasets = set()
        for keyword in target_keywords:
            if keyword:
                query = (
                    f"{base_query} AND (:title: '{keyword}' "
                    f"OR :description: '{keyword}' OR :keyword: '{keyword}')"
                )
            else:
                query = base_query
            log.info("Query:")
            log.info(f"{query}")
            page = 1
            found_datasets_per_keyword = []
            data_query = {
                "order": "published_date",
                "search_for": query,
                "page": page,
                "page_size": MAX_HITS_PER_PAGE,
                "order_direction": "desc",
                "item_type": 3,  # datasets
            }
            # Search endpoint:
            # https://docs.figshare.com/#articles_search
            while True:
                results = api.query(endpoint="/articles/search", data=data_query)
                if results["status_code"] != 200:
                    log.warning(f"Failed to fetch page {page} for file extension {file_type['type']}")
                    log.warning(f"Status code: {results['status_code']}")
                    log.warning(f"Response headers: {results['headers']}")
                    log.warning(f"Response body: {results['response']}")
                    break
                response = results["response"]
                if not response or len(response) == 0:
                    break
                # Extract datasets ids.
                found_datasets_per_keyword_per_page = [hit["id"] for hit in response]
                found_datasets_per_keyword += found_datasets_per_keyword_per_page
                log.info(f"Page {page} fetched successfully (new datasets: {len(found_datasets_per_keyword_per_page)} / total:{len(found_datasets_per_keyword)}).")
                page += 1
                # Be gentle with Figshare servers
                # https://docs.figshare.com/#figshare_documentation_api_description_rate_limiting
                # "We recommend that clients use the API responsibly
                # and do not make more than one request per second."
                time.sleep(1)
            found_datasets.update(found_datasets_per_keyword)
            sys.exit(1)
        # Extract info for all datasets.
        datasets_lst = []
        texts_lst = []
        files_lst = []
        datasets_count_old = datasets_df.shape[0]
        for dataset_id in found_datasets:
            resp_json_article = request_figshare_dataset_with_id(dataset_id)
            if not resp_json_article:
                continue
            dataset_info, text_info, file_info = extract_records(resp_json_article)
            datasets_lst += dataset_info
            texts_lst += text_info
            files_lst += file_info
            # Merge datasets.
            datasets_df_tmp = pd.DataFrame(datasets_lst)
            datasets_df = pd.concat([datasets_df, datasets_df_tmp], ignore_index=True)
            datasets_df.drop_duplicates(
                subset=["dataset_origin", "dataset_id"], keep="first", inplace=True
            )
            # Merge texts.
            texts_df_tmp = pd.DataFrame(texts_lst)
            texts_df = pd.concat([texts_df, texts_df_tmp], ignore_index=True)
            texts_df.drop_duplicates(
                subset=["dataset_origin", "dataset_id"], keep="first", inplace=True
            )
            # Merge files.
            files_df_tmp = pd.DataFrame(files_lst)
            files_df = pd.concat([files_df, files_df_tmp], ignore_index=True)
            files_df.drop_duplicates(
                subset=["dataset_id", "file_name", "file_md5"],
                keep="first",
                inplace=True,
            )

        print(
            f"Number of datasets found: {len(datasets_lst)} ({datasets_df.shape[0] - datasets_count_old} new)"
        )
        print(f"Number of files found: {len(files_lst)}")
        print("-" * 30)

    print(f"Total number of datasets found: {datasets_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    # Save dataframes to disk
    datasets_export_path = pathlib.Path(arg.output) / "figshare_datasets.tsv"
    datasets_df.to_csv(datasets_export_path, sep="\t", index=False)
    print(f"Results saved in {str(datasets_export_path)}")
    texts_export_path = pathlib.Path(arg.output) / "figshare_datasets_text.tsv"
    texts_df.to_csv(texts_export_path, sep="\t", index=False)
    print(f"Results saved in {str(texts_export_path)}")
    files_df = toolbox.remove_excluded_files(files_df, EXCLUDED_FILES, EXCLUDED_PATHS)
    files_export_path = pathlib.Path(arg.output) / "figshare_files.tsv"
    files_df.to_csv(files_export_path, sep="\t", index=False)
    print(f"Results saved in {str(files_export_path)}")
    print("-" * 30)

    if scrap_zip:
        # Scrap zip files content.
        zip_df = scrap_figshare_zip_content(files_df)
        # We don't remove duplicates here because
        # one zip file can contain several files with the same name
        # but within different folders.
        files_df = pd.concat([files_df, zip_df], ignore_index=True)
        print(f"Number of files found inside zip files: {zip_df.shape[0]}")
        print(f"Total number of files found: {files_df.shape[0]}")
        files_df = toolbox.remove_excluded_files(
            files_df, EXCLUDED_FILES, EXCLUDED_PATHS
        )
        files_df.to_csv(files_export_path, sep="\t", index=False)
        print(f"Results saved in {str(files_export_path)}")
        print("-" * 30)

    return files_export_path, datasets_export_path, texts_export_path


if __name__ == "__main__":
    # Parse input arguments.
    ARGS = toolbox.get_scraper_cli_arguments()

    # Create output directory.
    toolbox.verify_output_directory(ARGS.output)

    # Create logger.
    log = create_logger(f"{ARGS.output}/scrap_figshare.log")

    # Print script name and doctring.
    log.info(__file__)
    log.info(__doc__)

    # Load tokens.
    toolbox.load_token()

    # Create API object.
    api = FigshareAPI(token=os.getenv("FIGSHARE_TOKEN"))
    # Test token validity.
    if api.is_token_valid():
        log.info("Figshare token is valid!")
    else:
        log.error("Figshare token is invalid!")
        log.error("Exiting script.")
        sys.exit(1)

    # Scrap Figshare
    FILES_EXPORT_PATH, DATASETS_EXPORT_PATH, TEXTS_EXPORT_PATH = main_scrap_figshare(
        api, ARGS, scrap_zip=True
    )

    # Remove datasets that contain non-MD related files
    # that come from zip files.
    # Read parameter file.
    FILE_TYPES, _, _, _ = toolbox.read_query_file(ARGS.query)
    # List file types from the query parameter file.
    FILE_TYPES_LST = [file_type["type"] for file_type in FILE_TYPES]
    # Zip is not a MD-specific file type.
    FILE_TYPES_LST.remove("zip")
    # Find false-positive datasets.
    FALSE_POSITIVE_DATASETS = toolbox.find_false_positive_datasets(
        FILES_EXPORT_PATH, DATASETS_EXPORT_PATH, FILE_TYPES_LST
    )
    # Clean files.
    toolbox.remove_false_positive_datasets(
        FILES_EXPORT_PATH, "files", FALSE_POSITIVE_DATASETS
    )
    toolbox.remove_false_positive_datasets(
        DATASETS_EXPORT_PATH, "datasets", FALSE_POSITIVE_DATASETS
    )
    toolbox.remove_false_positive_datasets(
        TEXTS_EXPORT_PATH, "texts", FALSE_POSITIVE_DATASETS
    )

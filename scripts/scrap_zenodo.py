"""Scrap molecular dynamics datasets and files from Zenodo."""

from datetime import datetime
import os
import pathlib
import time


from bs4 import BeautifulSoup
import dotenv
import pandas as pd
import requests


import toolbox


def normalize_file_size(file_str):
    """Normalize file size in bytes.

    Parameters
    ----------
    file_str : str
        File size with unit.
        For instance: 1.8 GB, 108.7 kB

    Returns
    -------
    int
        File size in bytes.
    """
    size, unit = file_str.split()
    if unit == "GB":
        size_in_bytes = float(size) * 1_000_000_000
    elif unit == "MB":
        size_in_bytes = float(size) * 1_000_000
    elif unit == "kB":
        size_in_bytes = float(size) * 1_000
    elif unit == "Bytes":
        size_in_bytes = float(size)
    else:
        size_in_bytes = 0
    return int(size_in_bytes)


def extract_data_from_zip_file(url, token):
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
    response = requests.get(url, params={"access_token": token})

    if response.status_code != 200:
        print(f"Error with URL: {url}")
        print(f"Status code: {response.status_code}")
        print(response.headers)
        return []
    soup = BeautifulSoup(response.content, "html5lib")
    if "Zipfile is not previewable" in response.text:
        print(f"No preview available for {url}")
        return []
    table = soup.find("ul", attrs={"class": "tree list-unstyled"})
    file_info = []
    for row in table.findAll("span"):
        file_info.append(row.text)
    file_lst = []
    for idx in range(0, len(file_info), 2):
        file_name = file_info[idx].strip()
        file_size_raw = file_info[idx + 1].strip()
        file_size = normalize_file_size(file_size_raw)
        file_dict = {"file_name": file_name, "file_size": file_size}
        file_dict["file_type"] = "none"
        if "." in file_name:
            file_dict["file_type"] = file_name.split(".")[-1].lower()
        file_lst.append(file_dict)
    return file_lst


def read_zenodo_token():
    """Read Zenodo token from disk.

    Returns
    -------
    str
        Zenodo token.
    """
    dotenv.load_dotenv(".env")
    return os.environ.get("ZENODO_TOKEN", "token_not_found")


def test_zenodo_connection(token, show_headers=False):
    """Test connection to Zenodo API.

    Parameters
    ----------
    token : str
        Token for Zenodo API
    show_headers : bool
        Default: False
        If true, prints HTTP response headers

    Zenodo HTTP status codes are listed here:
    https://developers.zenodo.org/#http-status-codes

    Parameters
    ----------
    token : str
        Token for Zenodo API
    """
    print("Trying connection to Zenodo...")
    # Basic Zenodo query
    response = requests.get(
        "https://zenodo.org/api/deposit/depositions",
        params={"access_token": token},
    )
    # Status code should be 200
    print(f"Status code: {response.status_code}", end="")
    if response.status_code == 200:
        print(" success!")
    else:
        print(" failed!")
    if show_headers:
        print(response.headers)


def search_zenodo_with_query(query, token, page=1, hits_per_page=10):
    """Search for datasets.

    Arguments
    ---------
    query: str
        Query.
    token : str
        Zenodo token.
    page: int
        Page number.
    hits_per_page: int
        Number of hits per pages.

    Returns
    -------
    dict
        Zenodo response as a JSON object.
    """
    response = requests.get(
        "https://zenodo.org/api/records",
        params={
            "q": query,
            "size": hits_per_page,
            "page": page,
            "status": "published",
            "access_token": token,
        },
    )
    return response.json()


def scrap_zip_content(files_df):
    """Scrap information from files contained in zip archives.

    Zenodo provides a preview only for the first 1000 files within a zip file.
    See:
    https://github.com/inveniosoftware/invenio-previewer/blob/
    8ecdd4299b80a83d39679859fcedae47d68b012c/invenio_previewer/
    extensions/zip.py#L28

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
    print("Number of zip files to scrap content from: " f"{zip_files_df.shape[0]}")
    for zip_idx in zip_files_df.index:
        zip_file = zip_files_df.loc[zip_idx]
        zip_counter += 1
        # According to Zenodo documentation.
        # https://developers.zenodo.org/#rate-limiting
        # One can run 60 or 100 requests per minute.
        # To be careful, we wait 60 secondes every 60 requests.
        sleep_time = 60
        if zip_counter % 60 == 0:
            print(
                f"Scraped {zip_counter} zip files / "
                f"{zip_files_df.shape[0]}\n"
                f"Waiting for {sleep_time} seconds..."
            )
            time.sleep(sleep_time)
        URL = (
            f"https://zenodo.org/record/{zip_file['dataset_id']}"
            f"/preview/{zip_file.loc['file_name']}"
        )
        # print(zip_counter, URL)
        files_tmp = extract_data_from_zip_file(URL, ZENODO_TOKEN)
        if files_tmp == []:
            continue
        # Add common extra fields
        for idx in range(len(files_tmp)):
            files_tmp[idx]["dataset_origin"] = zip_file.loc["dataset_origin"]
            files_tmp[idx]["dataset_id"] = zip_file["dataset_id"]
            files_tmp[idx]["from_zip_file"] = True
            files_tmp[idx]["origin_zip_file"] = zip_file.loc["file_name"]
            files_tmp[idx]["file_url"] = ""
            files_tmp[idx]["file_md5"] = ""
        files_in_zip_lst += files_tmp
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    return files_in_zip_df


def extract_records(response_json):
    """Extract information from the Zenodo records.

    Arguments
    ---------
    response_json: dict
        JSON object obtained after a request on Zenodo API.

    Returns
    -------
    datasets: list
        List of dictionnaries. Information on datasets.
    texts: list
        List of dictionnaries. Textual information on datasets
    files: list
        List of dictionnaies. Information on files.
    """
    datasets = []
    texts = []
    files = []
    if response_json["hits"]["hits"]:
        for hit in response_json["hits"]["hits"]:
            if hit["metadata"]["access_right"] != "open":
                continue
            dataset_id = str(hit["id"])
            dataset_dict = {
                "dataset_origin": "zenodo",
                "dataset_id": dataset_id,
                "doi": hit["doi"],
                "date_creation": toolbox.extract_date(hit["created"]),
                "date_last_modified": toolbox.extract_date(hit["updated"]),
                "date_fetched": datetime.now().isoformat(timespec="seconds"),
                "file_number": len(hit["files"]),
                "download_number": int(hit["stats"]["downloads"]),
                "view_number": int(hit["stats"]["views"]),
                "license": hit["metadata"]["license"]["id"],
                "dataset_url": f"https://zenodo.org/record/{dataset_id}",
            }
            datasets.append(dataset_dict)
            text_dict = {
                "dataset_origin": dataset_dict["dataset_origin"],
                "dataset_id": dataset_dict["dataset_id"],
                "title": toolbox.clean_text(hit["metadata"]["title"]),
                "author": toolbox.clean_text(hit["metadata"]["creators"][0]["name"]),
                "keywords": "none",
                "description": toolbox.clean_text(hit["metadata"]["description"]),
            }
            if "keywords" in hit["metadata"]:
                text_dict["keywords"] = ";".join(
                    [str(keyword) for keyword in hit["metadata"]["keywords"]]
                )
            # Handle existing but empty keywords.
            # For instance: https://zenodo.org/record/3741678
            if text_dict["keywords"] == "":
                text_dict["keywords"] = "none"
            texts.append(text_dict)
            for file_in in hit["files"]:
                file_dict = {
                    "dataset_origin": dataset_dict["dataset_origin"],
                    "dataset_id": dataset_dict["dataset_id"],
                    "file_type": file_in["type"],
                    "file_size": int(file_in["size"]),  # File size in bytes.
                    "file_md5": file_in["checksum"].removeprefix("md5:"),
                    "from_zip_file": False,
                    "file_name": file_in["key"],
                    "file_url": file_in["links"]["self"],
                    "origin_zip_file": "none",
                }
                files.append(file_dict)
    return datasets, texts, files


if __name__ == "__main__":
    ARGS = toolbox.get_scraper_cli_arguments()

    # Read Zenodo token
    ZENODO_TOKEN = read_zenodo_token()
    test_zenodo_connection(ZENODO_TOKEN)

    # Read parameter file
    (
        FILE_TYPES,
        MD_KEYWORDS,
        GENERIC_KEYWORDS,
        EXCLUDED_FILES,
        EXCLUDED_PATHS,
    ) = toolbox.read_query_file(ARGS.query_file)
    # Build query part with keywords.
    # We want something like:
    # AND ("KEYWORD 1" OR "KEYWORD 2" OR "KEYWORD 3")
    QUERY_MD_KEYWORDS = ' AND ("' + '" OR "'.join(MD_KEYWORDS) + '")'
    QUERY_GENERIC_KEYWORDS = ' AND ("' + '" OR "'.join(GENERIC_KEYWORDS) + '")'

    # Verify output directory exists
    toolbox.verify_output_directory(ARGS.output)

    # There is a hard limit of the number of hits
    # one can get from a single query.
    MAX_HITS_PER_QUERY = 10_000

    # The best strategy is to use paging.
    MAX_HITS_PER_PAGE = 1_000

    datasets_df = pd.DataFrame()
    texts_df = pd.DataFrame()
    files_df = pd.DataFrame()
    for file_type in FILE_TYPES:
        print(f"Looking for filetype: {file_type['type']}")
        query_records = []
        query_files = []
        query = f'resource_type.type:"dataset" ' f'AND filetype:"{file_type["type"]}"'
        if file_type["keywords"] == "md_keywords":
            query += QUERY_MD_KEYWORDS
        elif file_type["keywords"] == "generic_keywords":
            query += QUERY_GENERIC_KEYWORDS
        print(f"Query:\n{query}")
        # First get the total number of hits for a given query.
        resp_json = search_zenodo_with_query(query, ZENODO_TOKEN, hits_per_page=1)
        total_hits = resp_json["hits"]["total"]
        print(f"Number of hits: {total_hits}")
        page_max = total_hits // MAX_HITS_PER_PAGE + 1
        # Then, slice the query by page.
        for page in range(1, page_max + 1):
            print(f"Page: {page}")
            resp_json = search_zenodo_with_query(
                query, ZENODO_TOKEN, page=page, hits_per_page=MAX_HITS_PER_PAGE
            )
            datasets_tmp, texts_tmp, files_tmp = extract_records(resp_json)
            # Merge datasets
            datasets_df_tmp = pd.DataFrame(datasets_tmp)
            datasets_df = pd.concat([datasets_df, datasets_df_tmp], ignore_index=True)
            datasets_df.drop_duplicates(
                subset=["dataset_origin", "dataset_id"], keep="first", inplace=True
            )
            # Merge dataset texts
            texts_df_tmp = pd.DataFrame(texts_tmp)
            texts_df = pd.concat([texts_df, texts_df_tmp], ignore_index=True)
            texts_df.drop_duplicates(
                subset=["dataset_origin", "dataset_id"], keep="first", inplace=True
            )
            # Merge files
            files_df_tmp = pd.DataFrame(files_tmp)
            files_df = pd.concat([files_df, files_df_tmp], ignore_index=True)
            files_df.drop_duplicates(
                subset=["dataset_id", "file_name"], keep="first", inplace=True
            )
            if page * MAX_HITS_PER_PAGE >= MAX_HITS_PER_QUERY:
                print("Max hits per query reached!")
                break
        print(f"Number of datasets found: {len(datasets_tmp)}")
        print(f"Number of files found: {len(files_tmp)}")
        print("-" * 30)

    print(f"Total number of datasets found: {datasets_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    # Save datasets dataframe to disk
    datasets_export_path = pathlib.Path(ARGS.output) / "zenodo_datasets.tsv"
    datasets_df.to_csv(datasets_export_path, sep="\t", index=False)
    print(f"Results saved in {str(datasets_export_path)}")
    # Save text datasets dataframe to disk
    texts_export_path = pathlib.Path(ARGS.output) / "zenodo_datasets_text.tsv"
    texts_df.to_csv(texts_export_path, sep="\t", index=False)
    print(f"Results saved in {str(texts_export_path)}")
    # Save files dataframe to disk
    files_df = toolbox.remove_excluded_files(files_df, EXCLUDED_FILES, EXCLUDED_PATHS)
    files_export_path = pathlib.Path(ARGS.output) / "zenodo_files.tsv"
    files_df.to_csv(files_export_path, sep="\t", index=False)
    print(f"Results saved in {str(files_export_path)}")

    # Scrap zip files content
    zip_df = scrap_zip_content(files_df)
    # We don't remove duplicates here because
    # one zip file can contain several files with the same name
    # but within different folders.
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    print(f"Number of files found inside zip files: {zip_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    files_df = toolbox.remove_excluded_files(files_df, EXCLUDED_FILES, EXCLUDED_PATHS)
    files_df.to_csv(files_export_path, sep="\t", index=False)
    print(f"Results saved in {str(files_export_path)}")

"""Scrap molecular dynamics datasets and files from Zenodo."""

from datetime import datetime, timedelta
import logging
from json import tool
import os
import pathlib
import sys
import time

from bs4 import BeautifulSoup
import dotenv
import pandas as pd
import requests
import httpx

import toolbox

# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info


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


def extract_license(metadata):
    """Extract license from metadata.
    
    Parameters
    ----------
    metadata : dict
        Metadata from Zenodo API.

    Returns
    -------
    str
        License.
        Empty string if no license found.
    """
    try:
        license = metadata["license"]["id"]
    except KeyError:
        license = ""
    return license


def get_files_structure_from_zip(ul):
    """Get files structure from zip file preview.

    Recursion based on:
    https://stackoverflow.com/questions/17850121/parsing-nested-html-list-with-beautifulsoup

    Example:
    https://zenodo.org/records/7756756/preview/Glycerol020.zip

    <ul class="tree list-unstyled">
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item0">Glycerol020 </a>
        </div>
      </div><ul id="tree_item0">
          
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item3">Em2 </a>
        </div>
      </div><ul id="tree_item3">
          
      <li>
        <div class="ui equal width grid">
          <div class="row">
          <i class="folder icon"></i> <a href="#tree_item15">Flow </a>
        </div>
      </div><ul id="tree_item15">
          
      <li>
        <div class="ui equal width grid">
        <div class="row">
          <div class="no-padding left floated column"><span><i class="file outline icon"></i></i> flow_00001.dat</span></div>
          <div class="no-padding right aligned column">4.6 kB</div>
        </div>
      </div>
      </li>
      <li>
        <div class="ui equal width grid">
        <div class="row">
          <div class="no-padding left floated column"><span><i class="file outline icon"></i></i> flow_00003.dat</span></div>
          <div class="no-padding right aligned column">4.6 kB</div>
        </div>
      </div>
      </li>

    Parameters
    ----------
    ul : bs4.element
        HTML table containing the files structure.

    Returns
    -------
    dict
        Nested dictionary with files structure.
    """
    structure = {}
    for li in ul.find_all("li", recursive=False):
        # Extract folrder or file name.
        key = next(li.stripped_strings)
        ul = li.find("ul")
        if ul:
            structure[key] = get_files_structure_from_zip(ul)
        # Extract file size.
        elif li.find("div", attrs={"class": "no-padding right aligned column"}):
            structure[key] = li.find("div", attrs={"class": "no-padding right aligned column"}).text
        else:
            structure[key] = None
    return structure


def extract_data_from_zip_file(url):
    """Extract data from zip file preview.

    Examples of zip file previews:
    - https://zenodo.org/records/4444751/preview/code.zip
    - https://zenodo.org/records/16412906/preview/DPPS200_HN45_0.25M-NaCl_TIP3P_353.15K_prod.zip

    url : str
        URL of zip file preview

    Returns
    -------
    list
        List of dictionnaries with data extracted from zip preview.
    """
    file_lst = []
    response = toolbox.make_http_get_request_with_retries(url, max_attempts=5)
    if response is None:
        return file_lst
    if "Zipfile is not previewable" in response.text:
        print(f"No preview available for {url}")
        return file_lst
    # Scrap HTML content.
    soup = BeautifulSoup(response.content, "html5lib")
    table = soup.find("ul", attrs={"class": "tree list-unstyled"})
    files_structure = get_files_structure_from_zip(table)
    # Convert nested dictionnary files structure to a flat dictionnary.
    df = pd.json_normalize(files_structure, sep="/")
    # Handle case with zip file with no files.
    # For instance:
    # https://zenodo.org/records/15878278/preview/data_naresh.zip
    try:
        files_dict = df.to_dict(orient="records")[0]
    except IndexError:
        return file_lst
    # Normalize file size.
    for path, size in files_dict.items():
        if size:
            file_dict = {
                "file_name": path,
                "file_size": normalize_file_size(size),
                "file_type": toolbox.extract_file_extension(path),
            }
            file_lst.append(file_dict)
    print(f"Found {len(file_lst)} files.")
    return file_lst


def read_zenodo_token():
    """Read Zenodo token from disk.

    Returns
    -------
    str
        Zenodo token.
    """
    dotenv.load_dotenv(".env")
    if "ZENODO_TOKEN" in os.environ:
        print("Found Zenodo token.")
    else:
        print("Zenodo token is missing.")
    return os.environ.get("ZENODO_TOKEN", "")


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
    print(f"Status code: {response.status_code}")
    if response.status_code == 200:
        print("-> success!")
    else:
        print("-> failed!")
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
    time.sleep(1)
    response = httpx.get(
        url="https://zenodo.org/api/records",
        params={
            "q": query,
            "size": hits_per_page,
            "page": page,
            "status": "published",
            "access_token": token,
        },
        timeout=60.0,
        follow_redirects=True,
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
    print(f"Number of zip files to scrap content from: {zip_files_df.shape[0]}")
    # The Zenodo API does not provide any endpoint to get the content of zip files.
    # We use direct GET requests on the HTML preview of the zip files.
    # We wait 1.5 seconds between each request,
    # to be gentle with the Zenodo servers.
    for zip_idx in zip_files_df.index:
        zip_file = zip_files_df.loc[zip_idx]
        zip_counter += 1
        url = (
            f"https://zenodo.org/records/{zip_file['dataset_id']}"
            f"/preview/{zip_file.loc['file_name']}"
        )
        # print(zip_counter, URL)
        time.sleep(1.5)
        files_tmp = extract_data_from_zip_file(url)
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
        print(
                f"Scraped {zip_counter} zip files "
                f"({zip_files_df.shape[0] - zip_counter} remaining)"
            )
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
            # 'hit' is a Python dictionary.
            if hit["metadata"]["access_right"] != "open":
                continue
            dataset_id = str(hit["id"])
            print(f"Extracting metadata for dataset: {dataset_id}")
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
                "license": extract_license(hit["metadata"]),
                "dataset_url": hit["links"]["self_html"],
            }
            datasets.append(dataset_dict)
            text_dict = {
                "dataset_origin": dataset_dict["dataset_origin"],
                "dataset_id": dataset_dict["dataset_id"],
                "title": toolbox.clean_text(hit["metadata"]["title"]),
                "author": toolbox.clean_text(hit["metadata"]["creators"][0]["name"]),
                "keywords": "none",
                "description": toolbox.clean_text(hit["metadata"].get("description", "")),
            }
            if "keywords" in hit["metadata"]:
                text_dict["keywords"] = ";".join(
                    [str(keyword) for keyword in hit["metadata"]["keywords"]]
                )
            # Handle existing but empty keywords.
            # For instance: https://zenodo.org/records/3741678
            if text_dict["keywords"] == "":
                text_dict["keywords"] = "none"
            texts.append(text_dict)
            for file_in in hit["files"]:
                file_dict = {
                    "dataset_origin": dataset_dict["dataset_origin"],
                    "dataset_id": dataset_dict["dataset_id"],
                    "file_size": int(file_in["size"]),  # File size in bytes.
                    "file_md5": file_in["checksum"].removeprefix("md5:"),
                    "from_zip_file": False,
                    "file_name": file_in["key"],
                    "file_type": toolbox.extract_file_extension(file_in["key"]),
                    "file_url": file_in["links"]["self"],
                    "origin_zip_file": "none",
                }
                # Some file types could be empty.
                # See for instance file "lmp_mpi" in:
                # https://zenodo.org/record/5797177
                # https://zenodo.org/api/records/5797177
                # Set these file types to "none".
                if file_dict["file_type"] == "":
                    file_dict["file_type"] = "none"
                files.append(file_dict)
    return datasets, texts, files


if __name__ == "__main__":
    start_time = time.perf_counter()
    ARGS = toolbox.get_scraper_cli_arguments()

    # Create logger
    log_file = logging.FileHandler(f"{ARGS.output}/scrap_zenodo.log", mode="w")
    log_file.setLevel(logging.INFO)
    log_stream = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_file, log_stream],
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    # Rewire the print function to logging.info
    print = logging.info

    # Print script name and doctring
    print(__file__)
    print(__doc__)

    # Read Zenodo token
    ZENODO_TOKEN = read_zenodo_token()
    if ZENODO_TOKEN == "":
        print("No Zenodo token found.")
        sys.exit(1)
    test_zenodo_connection(ZENODO_TOKEN)

    # Read parameter file
    (FILE_TYPES, KEYWORDS, EXCLUDED_FILES, EXCLUDED_PATHS) = toolbox.read_query_file(
        ARGS.query
    )
    # Build query part with keywords.
    # We want something like:
    # AND ("KEYWORD 1" OR "KEYWORD 2" OR "KEYWORD 3")
    QUERY_KEYWORDS = ' AND ("' + '" OR "'.join(KEYWORDS) + '")'

    # Verify output directory exists
    toolbox.verify_output_directory(ARGS.output)

    # There is a hard limit of the number of hits
    # one can get from a single query.
    MAX_HITS_PER_QUERY = 10_000

    # The best strategy is to use paging.
    MAX_HITS_PER_PAGE = 100
    print(f"Max hits per page: {MAX_HITS_PER_PAGE}")

    datasets_df = pd.DataFrame()
    texts_df = pd.DataFrame()
    files_df = pd.DataFrame()
    print("-" * 30)
    for file_type in FILE_TYPES:
        print(f"Looking for filetype: {file_type['type']}")
        datasets_count_old = datasets_df.shape[0]
        query_records = []
        query_files = []
        query = f"""resource_type.type:"dataset" AND filetype:"{file_type['type']}" """
        if file_type["keywords"] == "keywords":
            query += QUERY_KEYWORDS
        print("Query:")
        print(f"{query}")
        # First get the total number of hits for a given query.
        resp_json = search_zenodo_with_query(query, ZENODO_TOKEN, hits_per_page=1)
        total_hits = int(resp_json["hits"]["total"])
        print(f"Number of hits: {total_hits}")
        if total_hits == 0:
            print("-" * 30)
            continue
        page_max = total_hits // MAX_HITS_PER_PAGE + 1
        # Then, slice the query by page.
        for page in range(1, page_max + 1):
            print(f"Page {page}/{page_max} for filetype: {file_type['type']}")
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
        print(
            f"Number of datasets found: {len(datasets_tmp)} ({datasets_df.shape[0] - datasets_count_old} new)"
        )
        print(f"Number of files found: {len(files_tmp)}")
        print("-" * 30)

    print(f"Total number of datasets found: {datasets_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    # Save datasets dataframe to disk
    DATASETS_EXPORT_PATH = pathlib.Path(ARGS.output) / "zenodo_datasets.tsv"
    datasets_df.to_csv(DATASETS_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(DATASETS_EXPORT_PATH)}")
    # Save text datasets dataframe to disk
    TEXTS_EXPORT_PATH = pathlib.Path(ARGS.output) / "zenodo_datasets_text.tsv"
    texts_df.to_csv(TEXTS_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(TEXTS_EXPORT_PATH)}")
    # Save files dataframe to disk
    files_df = toolbox.remove_excluded_files(files_df, EXCLUDED_FILES, EXCLUDED_PATHS)
    FILES_EXPORT_PATH = pathlib.Path(ARGS.output) / "zenodo_files.tsv"
    files_df.to_csv(FILES_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(FILES_EXPORT_PATH)}")

    # Scrap zip files content.
    print("-" * 30)
    zip_df = scrap_zip_content(files_df)
    # We don't remove duplicates here because
    # one zip file can contain several files with the same name
    # but within different folders.
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    print(f"Number of files found inside zip files: {zip_df.shape[0]}")
    print(f"Total number of files found: {files_df.shape[0]}")
    files_df = toolbox.remove_excluded_files(files_df, EXCLUDED_FILES, EXCLUDED_PATHS)
    files_df.to_csv(FILES_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(FILES_EXPORT_PATH)}")
    print("-" * 30)

    # Remove datasets that contain non-MD related files
    # that come from zip files.
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
    # Script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    print(f"Scraping duration: {timedelta(seconds=elapsed_time)}")

"""Scrap molecular dynamics datasets and files from OSF."""

from datetime import datetime
import logging
import math
import os
import pathlib
import time


from bs4 import BeautifulSoup
import dotenv
import pandas as pd
import tqdm
import requests

try:
    import toolbox
except ModuleNotFoundError:
    from . import toolbox


# Rewire the print function from the toolbox module to logging.info
toolbox.print = logging.info


def read_osf_token():
    """Read OSF token from disk.

    Returns
    -------
    str
        Token for OSF API.
    """
    dotenv.load_dotenv(".env")
    return os.environ.get("OSF_TOKEN", "token_not_found")


def query_osf_api(
    token="",
    url="https://api.osf.io/v2/",
    params={},
    attempt_number=3,
    time_between_attempt=3,
    print_status_on_success=False,
    print_headers=False,
):
    """Query OSF API.

    Parameters
    ----------
    token : str, optional
        Token for OSF API.
    url : str, optional
        API endpoint
    params : dict, optional
        Parameters to pass to API endpoint.
    attempt_number : int, optional
        Number of attempt to try connection.
        Default: 3
    time_between_attempt : int, optional
        Number of seconds to way between attempts.
        Default: 3
    print_status_on_success : bool, optional
        Default: False
        If true, prints HTTP response status
    print_headers : bool, optional
        Default: False
        If true, prints HTTP response headers

    Returns
    -------
    json
        API response in JSON.
    """
    attempt = 1
    response = None
    while attempt <= attempt_number:
        try:
            response = requests.get(
                url, params=params, headers={"Authorization": f"Bearer {token}"}
            )
            # Count the number of times the OSF API is called
            query_osf_api.counter += 1
        except Exception as exc:
            print(f"Cannot establish connection to {url}")
            print(f"Exception type: {exc.__class__}")
            print(f"Exception message: {exc}\n")
            return {"error": {"detail": "Cannot established connection."}}
        if response.status_code == 200:
            break
        else:
            print(f"Error with URL: {url}")
            print(f"Status code: {response.status_code}")
            print(f"Attempt {attempt}/{attempt_number}")
            if attempt < attempt_number:
                print(f"Will retry in {time_between_attempt} seconds")
                time.sleep(time_between_attempt)
            else:
                print("Cannot access ressource. Aborting.")
                print(f"Headers: {response.headers}")
                return {"error": {"detail": "Status code is not 200."}}
        attempt += 1
    if print_status_on_success:
        print(f"Status code: {response.status_code} -> success")
    if print_headers:
        print(response.headers)
    return response.json()


def test_osf_connection(token):
    """Test connection to OSF API.

    Parameters
    ----------
    token : str
        Token for OSF API
    """
    print("Trying connection to OSF...")
    query_osf_api(
        token=token, url="https://api.osf.io/v2/users/me/", print_status_on_success=True
    )


def search_datasets(
    token, file_types, query_md_keywords, query_generic_keywords, excluded_files, excluded_paths
):
    """Search datasets relevant to file types and keywords.

    API endpoint: https://api.osf.io/v2/search/files/

    Parameters
    ----------
    token : str
        Token for OSF API
    file_types : dict
        Dictionnary with file type definitions.
    query_md_keywords : str
        Query string for MD specific keywords.
    query_generic_keywords : str
        Query string for MD keywords.
    excluded_files : list
        Patterns for file exclusion.
    excluded_paths : list
        Patterns for path exclusion.

    Returns
    -------
    Set
        Relevant dataset ids.
    """
    datasets = set()
    for file_type in file_types:
        print("-" * 30)
        print(f"Looking for filetype: {file_type['type']}")
        datasets_tmp = set()
        query = file_type["type"]
        if file_type["keywords"] == "md_keywords":
            query += query_md_keywords
        elif file_type["keywords"] == "generic_keywords":
            query += query_generic_keywords
        print("Query:")
        print(query)
        resp_json = query_osf_api(
            token=token,
            url="https://api.osf.io/v2/search/files/",
            params={"q": query, "page": 1},
        )
        results_total = resp_json["links"]["meta"]["total"]
        results_per_page = resp_json["links"]["meta"]["per_page"]
        page_max = math.ceil(results_total / results_per_page)
        pbar = tqdm.tqdm(
            range(1, page_max + 1),
            leave=True,
            bar_format="{l_bar}{n_fmt}/{total_fmt} pages",
        )
        for page in pbar:
            resp_json = query_osf_api(
                token=token,
                url="https://api.osf.io/v2/search/files/",
                params={"q": query, "page": page},
            )
            for file_info in resp_json["data"]:
                if file_info["attributes"]["kind"] != "file":
                    break
                if not file_info["attributes"]["name"].endswith(file_type["type"]):
                    break
                if file_info["relationships"]["target"]["data"]["type"] == "nodes":
                    datasets_tmp.add(file_info["relationships"]["target"]["data"]["id"])
        datasets_count_old = len(datasets)
        datasets.update(datasets_tmp)
        print(f"Number of datasets found: {len(datasets_tmp)} ({len(datasets) - datasets_count_old} new)")
    print("-" * 30)
    return datasets


def add_children_parent_datasets(token, dataset_ids):
    """Add children and parent datasets.

    API endpoint for children:
    - https://api.osf.io/v2/nodes/{datasetid}/children/
    - example: https://api.osf.io/v2/nodes/ugkwa/children/ (2 pages)

    API endpoint fo parent:
    - https://api.osf.io/v2/nodes/{datasetid}/
    - example: https://api.osf.io/v2/nodes/ugkwa/
    then look at the data -> relationships -> parent -> data property

    Parameters
    ----------
    token : str
        Token for OSF API
    dataset_ids : set
        Datasets ids

    Returns
    -------
    set
        Set of dataset ids.
    """
    dataset_ids_out = set()
    print("Looking for children and parent datasets")
    pbar = tqdm.tqdm(
        dataset_ids,
        leave=False,
        bar_format="{l_bar}{n_fmt}/{total_fmt}{postfix}",
    )
    for dataset_id in pbar:
        pbar.set_postfix({"dataset": str(dataset_id)})
        # Add current dataset
        dataset_ids_out.add(dataset_id)
        # Search children
        page = 1
        page_max = 2
        while page <= page_max:
            parameters = {"page": page}
            api_json = query_osf_api(
                token=token,
                url=f"https://api.osf.io/v2/nodes/{dataset_id}/children/",
                params=parameters,
            )
            if "error" in api_json:
                break
            results_total = api_json["links"]["meta"]["total"]
            results_per_page = api_json["links"]["meta"]["per_page"]
            page_max = math.ceil(results_total / results_per_page)
            for child in api_json["data"]:
                if child["type"] == "nodes":
                    dataset_ids_out.add(child["id"])
            page += 1
        # Search parent
        api_json = query_osf_api(
            token=token, url=f"https://api.osf.io/v2/nodes/{dataset_id}/"
        )
        if "error" in api_json:
            continue
        relationships = api_json["data"]["relationships"]
        if (
            "parent" in relationships
            and relationships["parent"]["data"]["type"] == "nodes"
        ):
            dataset_ids_out.add(relationships["parent"]["data"]["id"])
    print(
        f"Found {len(dataset_ids_out)-len(dataset_ids)} new children / parent datasets"
    )
    print(f"Total datasets: {len(dataset_ids_out)}")
    print("-" * 30)
    return dataset_ids_out


def query_datasets(token, datasets):
    """Index dataset informations.

    API endpoints:
    - node details: https://api.osf.io/v2/nodes/{datasetid}/
    - file storage: https://api.osf.io/v2/nodes/{datasetid}/files/
    - file list (first level): https://api.osf.io/v2/nodes/{datasetid}/files/osfstorage/

    Parameters
    ----------
    token : str
        Token for OSF API
    datasets : set
        Datasets ids

    Returns
    -------
    datasets: list
        List of dictionnaries. Information on datasets.
    texts: list
        List of dictionnaries. Textual information on datasets
    """
    datasets_lst = []
    texts_lst = []
    print("Scraping datasets information")
    pbar = tqdm.tqdm(
        datasets,
        leave=False,
        bar_format="{l_bar}{n_fmt}/{total_fmt}{postfix}",
    )
    for dataset_id in pbar:
        pbar.set_postfix({"dataset": str(dataset_id)})
        resp_json = query_osf_api(
            token=token, url=f"https://api.osf.io/v2/nodes/{dataset_id}/"
        )
        if "error" in resp_json:
            continue
        dataset_dict = {
            "dataset_origin": "osf",
            "dataset_id": dataset_id,
            "doi": "",
            "date_creation": toolbox.extract_date(
                resp_json["data"]["attributes"]["date_created"]
            ),
            "date_last_modified": toolbox.extract_date(
                resp_json["data"]["attributes"]["date_modified"]
            ),
            "date_fetched": datetime.now().isoformat(timespec="seconds"),
            "file_number": 0,
            "download_number": 0,
            "view_number": 0,
            # "license": resp_json["data"]["attributes"]["node_license"],
            "license": "",
            "dataset_url": f"https://osf.io/{dataset_id}/",
        }
        datasets_lst.append(dataset_dict)
        text_dict = {
            "dataset_origin": "osf",
            "dataset_id": dataset_id,
            "title": toolbox.clean_text(resp_json["data"]["attributes"]["title"]),
            "author": "",
            "keywords": "none",
            "description": toolbox.clean_text(
                resp_json["data"]["attributes"]["description"]
            ),
        }
        if resp_json["data"]["attributes"]["tags"]:
            text_dict["keywords"] = ";".join(
                [str(keyword) for keyword in resp_json["data"]["attributes"]["tags"]]
            )
        texts_lst.append(text_dict)
        # Get files URL
        resp_json = query_osf_api(
            token=token,
            url=f"https://api.osf.io/v2/nodes/{dataset_id}/files",
        )
        files_url = resp_json["data"][0]["relationships"]["files"]["links"]["related"][
            "href"
        ]
        datasets_lst[-1]["files_url"] = files_url
    print(f"Found information for {len(datasets_lst)} datasets")
    print("-" * 30)
    return datasets_lst, texts_lst


def index_files_from_all_datasets(token, datasets_df):
    files_lst = []
    print("Indexing datasets files")
    pbar = tqdm.tqdm(
        datasets_df.index,
        leave=True,
        bar_format="{l_bar}{n_fmt}/{total_fmt}{postfix}",
    )
    for index in pbar:
        dataset_id = datasets_df.loc[index, "dataset_id"]
        pbar.set_postfix({"dataset": dataset_id})
        url = datasets_df.loc[index, "files_url"]
        files_lst += index_files_from_one_dataset(token, dataset_id, url)
    print(f"Found {len(files_lst)} files")
    print("-" * 30)
    return files_lst


def index_files_from_one_dataset(token, dataset_id, dataset_files_url):
    """Index files from a single dataset.

    Parameters
    ----------
    token : str
        Token for OSF API.
    dataset_id : str
        Dataset id.
        Example: "8xuaj"
    dataset_files_url : str
        API endpoint to start to index files.
        Example: "https://api.osf.io/v2/nodes/8xuaj/files/osfstorage/"

    Returns
    -------
    list
        List of dictionnaries containing file descriptions.
    """
    files_lst = []
    query_urls_lst = [dataset_files_url]
    while query_urls_lst:
        target_url = query_urls_lst.pop(0)
        # Query at least one page
        page = 1
        page_max = 1
        while page <= page_max:
            parameters = {"page": page}
            api_resp = query_osf_api(token, target_url, params=parameters)
            if "error" in api_resp:
                break
            results_total = api_resp["links"]["meta"]["total"]
            results_per_page = api_resp["links"]["meta"]["per_page"]
            page_max = math.ceil(results_total / results_per_page)
            for files in api_resp["data"]:
                if files["attributes"]["kind"] == "folder":
                    query_urls_lst.append(
                        files["relationships"]["files"]["links"]["related"]["href"]
                    )
                if files["attributes"]["kind"] == "file":
                    file_dict = {
                        "dataset_origin": "osf",
                        "dataset_id": dataset_id,
                        "file_type": toolbox.extract_file_extension(
                            files["attributes"]["name"]
                        ),
                        "file_size": files["attributes"]["size"],  # File size in bytes.
                        "file_md5": files["attributes"]["extra"]["hashes"]["md5"],
                        "from_zip_file": False,
                        "file_name": files["attributes"]["materialized_path"],
                        "file_url": files["links"]["download"],
                        "origin_zip_file": "none",
                    }
                    # Remove / at beginning of file path
                    if file_dict["file_name"].startswith("/"):
                        file_dict["file_name"] = file_dict["file_name"][1:]
                    files_lst.append(file_dict)
            page += 1
    print(f"Files found in dataset {dataset_id}: {len(files_lst)}")
    return files_lst


if __name__ == "__main__":
    ARGS = toolbox.get_scraper_cli_arguments()

    # Create logger
    log_file = logging.FileHandler(f"{ARGS.output}/scrap_osf.log", mode="w")
    log_file.setLevel(logging.INFO)
    log_stream = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_file, log_stream],
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG
    )
    # Rewire the print function to logging.info
    print = logging.info

    # Print script name and doctring
    print(__file__)
    print(__doc__)

    # Rest API call counter
    query_osf_api.counter = 0

    # Read OSF token
    OSF_TOKEN = read_osf_token()
    test_osf_connection(OSF_TOKEN)

    # Read parameter file
    (
        FILE_TYPES,
        MD_KEYWORDS,
        GENERIC_KEYWORDS,
        EXCLUDED_FILES,
        EXCLUDED_PATHS,
    ) = toolbox.read_query_file(ARGS.query)
    # Build query part with keywords.
    # We want something like:
    # AND ("KEYWORD 1" OR "KEYWORD 2" OR "KEYWORD 3")
    QUERY_MD_KEYWORDS = ' AND ("' + '" OR "'.join(MD_KEYWORDS) + '")'
    QUERY_GENERIC_KEYWORDS = ' AND ("' + '" OR "'.join(GENERIC_KEYWORDS) + '")'

    # Verify output directory exists
    toolbox.verify_output_directory(ARGS.output)

    # Search datasets
    dataset_ids = search_datasets(
        OSF_TOKEN,
        FILE_TYPES,
        QUERY_MD_KEYWORDS,
        QUERY_GENERIC_KEYWORDS,
        EXCLUDED_FILES,
        EXCLUDED_PATHS,
    )

    # dataset_ids = {'8xuaj', 'hk9f7', 'rnc6d', 'hsp5w', '3awds', 'p3gsq'}
    # dataset_ids = {'3jap'}

    # In OSF, datasets are represented as "nodes"
    # some nodes have "children" and "parent" that are worth collecting
    # We run it twice to be as exhaustive as possible
    dataset_ids = add_children_parent_datasets(OSF_TOKEN, dataset_ids)
    dataset_ids = add_children_parent_datasets(OSF_TOKEN, dataset_ids)

    # Query datasets (called "nodes" in OSF)
    datasets_lst, texts_lst = query_datasets(OSF_TOKEN, dataset_ids)
    datasets_df = pd.DataFrame(datasets_lst)
    texts_df = pd.DataFrame(texts_lst)

    # Save datasets dataframe to disk
    DATASETS_EXPORT_PATH = pathlib.Path(ARGS.output) / "osf_datasets.tsv"
    datasets_df.drop(columns="files_url").to_csv(
        DATASETS_EXPORT_PATH, sep="\t", index=False
    )
    print(f"Results saved in {str(DATASETS_EXPORT_PATH)}")
    
    # Save text datasets dataframe to disk
    TEXTS_EXPORT_PATH = pathlib.Path(ARGS.output) / "osf_datasets_text.tsv"
    texts_df.to_csv(TEXTS_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(TEXTS_EXPORT_PATH)}")

    # Query files
    files_lst = index_files_from_all_datasets(OSF_TOKEN, datasets_df)
    files_df = pd.DataFrame(files_lst)

    # Save files dataframe to disk
    FILES_EXPORT_PATH = pathlib.Path(ARGS.output) / "osf_files.tsv"
    files_df.to_csv(FILES_EXPORT_PATH, sep="\t", index=False)
    print(f"Results saved in {str(FILES_EXPORT_PATH)}")
    print(f"Total number of API calls: {query_osf_api.counter}")
    print("-" * 30)

    # Remove datasets that contain non-MD related files
    # that come from zip files.
    # Find false-positive datasets
    FILE_TYPES_LST = [file_type["type"] for file_type in FILE_TYPES]
    # Zip is not a MD-specific file type.
    FILE_TYPES_LST.remove("zip")
    FALSE_POSITIVE_DATASETS = toolbox.find_false_positive_datasets(
        FILES_EXPORT_PATH,
        DATASETS_EXPORT_PATH,
        FILE_TYPES_LST
    )
    # Clean files
    toolbox.remove_false_positive_datasets(FILES_EXPORT_PATH, "files", FALSE_POSITIVE_DATASETS)
    toolbox.remove_false_positive_datasets(DATASETS_EXPORT_PATH, "datasets", FALSE_POSITIVE_DATASETS)
    toolbox.remove_false_positive_datasets(TEXTS_EXPORT_PATH, "texts", FALSE_POSITIVE_DATASETS)

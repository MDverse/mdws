"""Scrap molecular dynamics datasets and files from Zenodo."""

import json
import logging
import os
import pathlib
import sys
import time
from datetime import datetime, timedelta

import logger
import loguru
import pandas as pd
import toolbox
from bs4 import BeautifulSoup
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)


def get_rate_limit_info(
    url_lst: list[str], token: str, logger: "loguru.Logger" = loguru.logger
) -> None:
    """Get rate limit information from Zenodo API endpoints.

    Parameters
    ----------
    url_lst : list of str
        List of URLs to send HEAD requests to.
    token : str
        Zenodo API token.
    logger : loguru.Logger, optional
        Logger for logging messages.
    """
    for url in url_lst:
        response = toolbox.make_http_get_request_with_retries(
            url=url,
            params={"token": token},
            timeout=60,  # Zenodo serveur can be sometimes slow to respond.
            max_attempts=1,
            delay_before_request=2,
            logger=logger,
        )
        if response is None:
            logger.error(f"Cannot connect to: {url}")
            continue
        logger.info(f"Rate limit info from {url}:")
        logger.info(
            "Header x-ratelimit-limit: "
            f"{response.headers.get('X-ratelimit-limit', None)}"
        )
        logger.info(
            "Header x-ratelimit-remaining: "
            f"{response.headers.get('X-ratelimit-remaining', None)}"
        )
        logger.info(
            "Header x-ratelimit-reset: "
            f"{response.headers.get('X-ratelimit-reset', None)}"
        )
        logger.info(f"Header retry-after: {response.headers.get('retry-after', None)}")


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
        license_name = metadata["license"]["id"]
    except KeyError:
        license_name = ""
    return license_name


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
            structure[key] = li.find(
                "div", attrs={"class": "no-padding right aligned column"}
            ).text
        else:
            structure[key] = None
    return structure


def extract_data_from_zip_file(url, logger: "loguru.Logger" = loguru.logger):
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
    response = toolbox.make_http_get_request_with_retries(
        url, delay_before_request=2, timeout=30, max_attempts=5
    )
    if response is None:
        return file_lst
    if "Zipfile is not previewable" in response.text:
        logger.warning(f"No preview available for: {url}")
        logger.warning("Skipping zip file.")
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
    logger.success(f"Found {len(file_lst)} files.")
    return file_lst


def is_zenodo_connection_working(
    token: str, logger: "loguru.Logger" = loguru.logger
) -> bool:
    """Test connection to Zenodo API.

    Zenodo HTTP status codes are listed here:
    https://developers.zenodo.org/#http-status-codes

    Parameters
    ----------
    token : str
        Token for Zenodo API

    Returns
    -------
    bool
        True if connection is successful, False otherwise.
    """
    logger.info("Trying connection to Zenodo...")
    response = toolbox.make_http_get_request_with_retries(
        url="https://zenodo.org/api/deposit/depositions",
        params={"access_token": token},
        timeout=10.0,
        max_attempts=2,
    )
    if not response:
        logger.error("Cannot connect to the Zenodo API.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def scrap_zip_content(
    files_df, logger: "loguru.Logger" = loguru.logger
) -> pd.DataFrame:
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
    logger.info(f"Number of zip files to scrap content from: {zip_files_df.shape[0]}")
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
        files_tmp = extract_data_from_zip_file(
            url,
            logger=logger,
        )
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
        logger.info(
            f"Scraped {zip_counter} zip files "
            f"({zip_files_df.shape[0] - zip_counter} remaining)."
        )
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    return files_in_zip_df


def extract_records(
    response_json, logger: "loguru.Logger" = loguru.logger
) -> tuple[list, list]:
    """Extract information from the Zenodo records.

    Arguments
    ---------
    response_json: dict
        JSON object obtained after a request on Zenodo API.

    Returns
    -------
    datasets: list
        List of dictionnaries. Information on datasets.
    files: list
        List of dictionnaies. Information on files.
    """
    datasets = []
    files = []
    if response_json["hits"]["hits"]:
        for hit in response_json["hits"]["hits"]:
            # 'hit' is a Python dictionary.
            if hit["metadata"]["access_right"] != "open":
                continue
            dataset_id = str(hit["id"])
            logger.info(f"Extracting metadata for dataset id: {dataset_id}")
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
                "title": toolbox.clean_text(hit["metadata"]["title"]),
                "author": toolbox.clean_text(hit["metadata"]["creators"][0]["name"]),
                "keywords": "none",
                "description": toolbox.clean_text(
                    hit["metadata"].get("description", "")
                ),
            }
            if "keywords" in hit["metadata"]:
                dataset_dict["keywords"] = ";".join(
                    [str(keyword) for keyword in hit["metadata"]["keywords"]]
                )
            # Handle existing but empty keywords.
            # For instance: https://zenodo.org/records/3741678
            if dataset_dict["keywords"] == "":
                dataset_dict["keywords"] = "none"
            datasets.append(dataset_dict)
            logger.info(f"Dataset URL: {dataset_dict['dataset_url']}")
            for file_in in hit["files"]:
                file_dict = {
                    "dataset_origin": dataset_dict["dataset_origin"],
                    "dataset_id": dataset_dict["dataset_id"],
                    "dataset_url": dataset_dict["dataset_url"],
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
    return datasets, files


def search_zenodo(
    query: str,
    ctx: toolbox.ContextManager,
    page: int = 1,
    number_of_results: int = 1,
) -> dict | None:
    """Get total number of hits for a given query.

    Parameters
    ----------
    query : str
        The search query string.
    ctx : toolbox.ContextManager
        Context manager containing configuration and logger.

    Returns
    -------
    response_json : dict
        JSON response from the Zenodo API.
    """
    params = {
        "q": query,
        "size": number_of_results,
        "page": page,
        "status": "published",
        "access_token": ctx.token,
    }
    response_json = None
    response = toolbox.make_http_get_request_with_retries(
        url="https://zenodo.org/api/records",
        params=params,
        timeout=60.0,
        logger=ctx.logger,
        delay_before_request=2,
        max_attempts=5,
    )
    if response is None:
        ctx.logger.warning("Failed to get response from the Zenodo API.")
        ctx.logger.warning("Getting next file type...")
        return None
    # Try to decode JSON response.
    try:
        response_json = response.json()
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        ctx.logger.warning("Failed to decode JSON response from the Zenodo API.")
        ctx.logger.warning(f"Error: {exc}")
        return None
    # Try to extract hits (= results).
    try:
        _ = response_json["hits"]
        _ = int(response_json["hits"]["total"])
    except (KeyError, ValueError):
        ctx.logger.warning("Cannot extract hits for HTTP response.")
        ctx.logger.debug("Response JSON")
        ctx.logger.debug(response_json)
        return None
    return response_json


def merge_dataframes_remove_duplicates(
    df1: pd.DataFrame, df2: pd.DataFrame, on_columns: list[str] | None = None
) -> pd.DataFrame:
    """Merge two dataframes and remove duplicates.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe.
    df2 : pd.DataFrame
        Second dataframe.
    on_columns : list of str, optional
        List of columns to consider for duplicates.
        If None, all columns are considered.

    Returns
    -------
    pd.DataFrame
        Merged dataframe with duplicates removed.
    """
    df_concat = pd.concat([df1, df2], ignore_index=True)
    return df_concat.drop_duplicates(subset=on_columns, keep="first")


def search_all_datasets(
    file_types: list[dict],
    keywords: list[str],
    ctx: toolbox.ContextManager,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Search all datasets on Zenodo.

    Parameters
    ----------
    file_types : list of dict
        List of file types to search for.
        Each dict contains:
        - type: str, file extension
        - keywords: str, "keywords" or "none"
    keywords : list of str
        List of keywords to use in the search.
    ctx : toolbox.ContextManager
        Context manager containing configuration and logger.

    Returns
    -------
    datasets_df : pd.DataFrame
        Dataframe with information on datasets.
    files_df : pd.DataFrame
        Dataframe with information on files.
    """
    # There is a hard limit of the number of hits
    # one can get from a single query.
    max_hits_per_query = 10_000
    # We used paging with max_hits_per_page per page.
    max_hits_per_page = 100
    # Build query part with keywords. We want something like:
    # AND ("KEYWORD 1" OR "KEYWORD 2" OR "KEYWORD 3")
    query_keywords = ' AND ("' + '" OR "'.join(keywords) + '")'
    # Create empty dataframes to store results.
    datasets_df = pd.DataFrame()
    files_df = pd.DataFrame()
    ctx.logger.info("-" * 30)
    for file_type in file_types:
        ctx.logger.info(f"Looking for filetype: {file_type['type']}")
        datasets_count_old = datasets_df.shape[0]
        # Build query with file type and optional keywords.
        query = f"""resource_type.type:"dataset" AND filetype:"{file_type["type"]}" """
        if file_type["keywords"] == "keywords":
            query += query_keywords
        ctx.logger.info("Query:")
        ctx.logger.info(f"{query}")
        # First, get the total number of hits for a given query.
        # This is needed to compute the number of pages of results to get.
        json_response = search_zenodo(query, ctx, page=1, number_of_results=1)
        if json_response is None or int(json_response["hits"]["total"]) == 0:
            ctx.logger.error("Getting next file type...")
            ctx.logger.info("-" * 30)
            continue
        total_hits = int(json_response["hits"]["total"])
        ctx.logger.info(f"Total hits for this query: {total_hits}")
        page_max = total_hits // max_hits_per_page + 1
        # Then, slice the query by page.
        for page in range(1, page_max + 1):
            json_response = search_zenodo(
                query, ctx, page=page, number_of_results=max_hits_per_page
            )
            ctx.logger.info(f"Page {page}/{page_max} for filetype: {file_type['type']}")
            if json_response is None:
                ctx.logger.warning("Failed to get response from the Zenodo API.")
                ctx.logger.warning("Getting next page...")
                continue
            datasets_tmp, files_tmp = extract_records(json_response, logger=ctx.logger)
            # Merge dataframes
            datasets_df = merge_dataframes_remove_duplicates(
                datasets_df,
                pd.DataFrame(datasets_tmp),
                on_columns=["dataset_origin", "dataset_id"],
            )
            files_df = merge_dataframes_remove_duplicates(
                files_df,
                pd.DataFrame(files_tmp),
                on_columns=["dataset_id", "file_name"],
            )
            ctx.logger.success(
                f"Found so far: {datasets_df.shape[0]:,} datasets "
                f"and {files_df.shape[0]:,} files"
            )
            if page * max_hits_per_page >= max_hits_per_query:
                ctx.logger.info("Max hits per query reached!")
                break
        ctx.logger.info(
            f"Number of datasets found: {len(datasets_tmp)} "
            f"({datasets_df.shape[0] - datasets_count_old} new)"
        )
        ctx.logger.info(f"Number of files found: {len(files_tmp)}")
        ctx.logger.info("-" * 30)
    ctx.logger.info(f"Total number of datasets found: {datasets_df.shape[0]}")
    ctx.logger.info(f"Total number of files found: {files_df.shape[0]}")
    return datasets_df, files_df


def main():
    """Scrap Zenodo datasets and files."""
    # Define data repository name.
    repository_name = "zenodo"
    # Keep track of script duration.
    start_time = time.perf_counter()
    # Parse input CLI arguments.
    args = toolbox.get_scraper_cli_arguments()
    # Create context manager.
    output_path = pathlib.Path(args.output) / repository_name
    output_path.mkdir(parents=True, exist_ok=True)
    context = toolbox.ContextManager(
        logger=logger.create_logger(
            logpath=f"{output_path}/{repository_name}_scraping.log"
        ),
        output_path=output_path,
        query_file_name=pathlib.Path(args.query),
    )
    # Log script name and doctring.
    context.logger.info(__file__)
    context.logger.info(__doc__)
    # Read and verify Zenodo token.
    load_dotenv()
    zenodo_token = os.environ.get("ZENODO_TOKEN", "")
    if not zenodo_token:
        context.logger.critical("No Zenodo token found.")
        context.logger.critical("Aborting.")
        sys.exit(1)
    else:
        context.logger.success("Found Zenodo token.")
        context.token = zenodo_token
    # Test connection to Zenodo API.
    if is_zenodo_connection_working(context.token, logger=context.logger):
        context.logger.success("Connection to Zenodo API successful.")
    else:
        context.logger.critical("Connection to Zenodo API failed.")
        context.logger.critical("Aborting.")
        sys.exit(1)
    # Get rate limit information.
    get_rate_limit_info(
        [
            "https://zenodo.org/api/records",
            "https://zenodo.org/records/4444751/preview/code.zip",
        ],
        zenodo_token,
        logger=context.logger,
    )
    # Read parameter file
    (file_types, keywords, excluded_files, excluded_paths) = toolbox.read_query_file(
        context.query_file_name,
        logger=context.logger,
    )
    # Verify output directory exists
    toolbox.verify_output_directory(context.output_path)

    datasets_df, files_df = search_all_datasets(file_types, keywords, context)

    # Scrap zip files content.
    context.logger.info("-" * 30)
    zip_df = scrap_zip_content(files_df, logger=context.logger)
    # We don't remove duplicates here because
    # one zip file can contain several files with the same name
    # but within different folders.
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    context.logger.info(f"Number of files found inside zip files: {zip_df.shape[0]}")
    context.logger.info(f"Total number of files found: {files_df.shape[0]}")
    files_df = toolbox.remove_excluded_files(files_df, excluded_files, excluded_paths)
    context.logger.info("-" * 30)

    # Remove datasets that contain non-MD related files
    # that come from zip files.
    datasets_df, files_df = toolbox.find_remove_false_positive_datasets(
        datasets_df, files_df, context
    )

    # Save dataframes to disk.
    toolbox.export_dataframe_to_parquet(
        "zenodo", toolbox.DataType.DATASETS, datasets_df, context
    )
    toolbox.export_dataframe_to_parquet(
        "zenodo", toolbox.DataType.FILES, files_df, context
    )

    # Script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    context.logger.info(f"Scraping Zenodo in: {timedelta(seconds=elapsed_time)}")


if __name__ == "__main__":
    main()

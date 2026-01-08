"""Scrap molecular dynamics datasets and files from Figshare."""

import json
import os
import pathlib
import sys
import time
from datetime import datetime, timedelta

import logger
import loguru
import pandas as pd
import toolbox
from figshare_api import FigshareAPI


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


def extract_files_from_json_response(
    json_dic: dict, file_list: list | None = None
) -> list[str]:
    """Walk recursively through the json directory tree structure.

    Examples
    --------
    https://figshare.com/ndownloader/files/3788686/preview/3788686/structure.json
    has 14 files in 2 levels of directories.

    Parameters
    ----------
    json_dic : dict
        JSON dictionary of zip file listing.

    file_list : list
        List with filenames

    Returns
    -------
    list
        List of filenames extracted from zip listing.
    """
    if file_list is None:
        file_list = []
    for value in json_dic["files"]:
        file_list.append(value["path"])
    for dir_list in json_dic["dirs"]:
        file_list = extract_files_from_json_response(dir_list, file_list)
    return file_list


def extract_files_from_zip_file(
    file_id: str, logger: "loguru.Logger" = loguru.logger, max_attempts: int = 3
) -> list[str]:
    """Extract files from a zip file content.

    No endpoint is available in the Figshare API.
    We perform a direct HTTP GET request to the zip file content url.

    Known issue with:
    https://figshare.com/ndownloader/files/31660220/preview/31660220/structure.json

    Parameters
    ----------
    file_id : str
        ID of the zip file to get content from.
    logger : loguru.Logger
        Logger object.
    max_attempts : int
        Maximum number of attempts to fetch the zip file content.

    Returns
    -------
    list
        List of file names contained in the zip file.
    """
    file_names = []
    url = (
        f"https://figshare.com/ndownloader/files/{file_id}"
        f"/preview/{file_id}/structure.json"
    )
    response = toolbox.make_http_get_request_with_retries(
        url=url,
        logger=logger,
        max_attempts=max_attempts,
        timeout=30,
        delay_before_request=2,
    )
    if response is None:
        logger.warning("Cannot get zip file content.")
        return file_names
    # Extract file names from JSON response.
    try:
        file_names = extract_files_from_json_response(response.json())
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.warning(f"Cannot extract files from JSON response: {exc}")
        logger.debug(f"Status code: {response.status_code}")
        logger.debug(response.text)
    logger.success(f"Found {len(file_names)} files.")
    return file_names


def get_stats_for_dataset(dataset_id: str) -> dict:
    """Get download stats for articles.

    Docs: https://info.figshare.com/user-guide/usage-metrics-and-statistics/
    Example:
    - id: 30950858
    - url: https://figshare.com/articles/software/Supplementary_files_for_MD_simulations_of_PcncAAAD/30950858
    - downloads: https://stats.figshare.com/total/downloads/article/30950858
    - views: https://stats.figshare.com/total/views/article/30950858

    Arguments
    ---------
    dataset_id: str
        Dataset id.

    Returns
    -------
    dict
        Figshare response as a JSON object.
    """
    stats = {"downloads": None, "views": None}
    for stat in stats:
        response = toolbox.make_http_get_request_with_retries(
            url=f"https://stats.figshare.com/total/{stat}/article/{dataset_id}",
            logger=logger,
            max_attempts=2,
            timeout=30,
            delay_before_request=2,
        )
        if response is None:
            continue
        # Extract stats from JSON response.
        try:
            stats[stat] = response.json()["totals"]
        except (json.decoder.JSONDecodeError, ValueError):
            logger.warning(f"Cannot extract '{stat}' for dataset id: {dataset_id}")
            continue
    return stats


def scrap_zip_files(
    files_df: pd.DataFrame, logger: "loguru.Logger" = loguru.logger
) -> pd.DataFrame:
    """Scrap information from files contained in zip archives.

    Uncertain how many files can be fetched from the preview.
    Only get file name and file type.
    File size and MD5 checksum are not available.

    Arguments
    ---------
    files_df: Pandas dataframe
        Dataframe with information about files.
    logger: loguru.Logger
        Logger object.

    Returns
    -------
    zip_df: Pandas dataframe
        Dataframe with information about files found in zip archive.
    """
    files_in_zip_lst = []
    zip_files_counter = 0
    zip_files_df = files_df[files_df["file_type"] == "zip"]
    logger.info(f"Number of zip files to scrap content from: {zip_files_df.shape[0]}")
    for zip_idx in zip_files_df.index:
        zip_file = zip_files_df.loc[zip_idx]
        file_id = zip_file["file_url"].split("/")[-1]
        zip_files_counter += 1
        # We cannot use the Figshare API to get the content of a zip file.
        # According to Figshare support
        # One can run 100 requests per 5 minutes (300 secondes).
        # To be careful, we wait 310 secondes every 100 requests.
        # SLEEP_TIME = 310
        # if zip_files_counter % 100 == 0:
        #     print(
        #         f"Scraped {zip_counter} zip files "
        #         f"({zip_files_df.shape[0] - zip_files_counter} remaining)"
        #     )
        #     print(f"Waiting for {SLEEP_TIME} seconds...")
        #     time.sleep(SLEEP_TIME)
        logger.info("Extracting files from zip file:")
        logger.info(zip_file["file_url"])
        file_names = extract_files_from_zip_file(file_id, logger)
        if file_names == []:
            logger.warning("No file found!")
            continue
        # Add other metadata.
        for name in file_names:
            file_metadata = {}
            file_metadata["dataset_origin"] = zip_file["dataset_origin"]
            file_metadata["dataset_id"] = zip_file["dataset_id"]
            file_metadata["dataset_url"] = zip_file["dataset_url"]
            file_metadata["file_name"] = name
            file_metadata["file_type"] = toolbox.extract_file_extension(name)
            file_metadata["file_size"] = None
            file_metadata["file_md5"] = None
            file_metadata["is_from_zip_file"] = True
            file_metadata["containing_zip_file_name"] = zip_file["file_name"]
            file_metadata["file_url"] = zip_file["file_url"]
            files_in_zip_lst.append(file_metadata)
        logger.info(
            f"{zip_files_counter} zip files processed -> "
            f"{zip_files_df.shape[0] - zip_files_counter} remaining"
        )
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    logger.success("Done extracting files from zip archives.")
    return files_in_zip_df


def extract_metadata_from_single_dataset_record(
    record_json: dict,
) -> tuple[dict, list[dict]]:
    """Extract information from a Figshare dataset/article record.

    Example of record:
    https://api.figshare.com/v2/articles/5840706
    that corresponds to dataset:
    https://figshare.com/articles/dataset/M1_gro/5840706

    Arguments
    ---------
    record_json: dict
        JSON object obtained after a request on FigShare API.

    Returns
    -------
    dict
        Dataset metadata.
    list
        List of files metadata.
    """
    dataset_info = {}
    files_info = []
    if record_json["is_embargoed"]:
        return dataset_info, files_info
    dataset_id = str(record_json["id"])
    # Disable stats for now.
    # dataset_stats = get_stats_for_dataset(dataset_id)
    dataset_stats = {"downloads": None, "views": None}
    dataset_info = {
        "dataset_origin": "figshare",
        "dataset_id": dataset_id,
        "doi": record_json["doi"],
        "date_creation": extract_date(record_json["created_date"]),
        "date_last_modified": extract_date(record_json["modified_date"]),
        "date_fetched": datetime.now().isoformat(timespec="seconds"),
        "file_number": len(record_json["files"]),
        "download_number": dataset_stats["downloads"],
        "view_number": dataset_stats["views"],
        "license": record_json["license"]["name"],
        "dataset_url": record_json["url_public_html"],
        "title": toolbox.clean_text(record_json["title"]),
        "author": toolbox.clean_text(record_json["authors"][0]["full_name"]),
        "keywords": "",
        "description": toolbox.clean_text(record_json["description"]),
    }
    # Add keywords only if any.
    if "tags" in record_json:
        dataset_info["keywords"] = ";".join(
            [toolbox.clean_text(keyword) for keyword in record_json["tags"]]
        )
    for file_in in record_json["files"]:
        if len(file_in["name"].split(".")) == 1:
            filetype = "none"
        else:
            filetype = file_in["name"].split(".")[-1].lower()
        file_dict = {
            "dataset_origin": dataset_info["dataset_origin"],
            "dataset_id": dataset_info["dataset_id"],
            "dataset_url": dataset_info["dataset_url"],
            "file_name": file_in["name"],
            "file_type": filetype,
            "file_size": file_in["size"],
            "file_md5": file_in["computed_md5"],
            "is_from_zip_file": False,
            "containing_zip_file_name": None,
            "file_url": file_in["download_url"],
        }
        files_info.append(file_dict)
    return dataset_info, files_info


def search_all_datasets(
    api: FigshareAPI, ctx: toolbox.ContextManager, max_hits_per_page: int = 100
) -> list[str]:
    """Search all Figshare datasets.

    We search datasets by iterating on:
    - file types
    - keywords (if any), one by one
    - pages of results

    Parameters
    ----------
    api : FigshareAPI
        Figshare API object.
    ctx : toolbox.ContextManager
        ContextManager object.
    max_hits_per_page : int
        Maximum number of hits per page.

    Returns
    -------
    list of str
        List of Figshare datasets ids.
    """
    # Read parameter file
    file_types, keywords, _, _ = toolbox.read_query_file(
        ctx.query_file_name, logger=ctx.logger
    )
    # We use paging to fetch all results.
    # we query max_hits_per_page hits per page.

    unique_datasets = []
    ctx.logger.info("-" * 30)
    for file_type in file_types:
        ctx.logger.info(f"Looking for filetype: {file_type['type']}")
        base_query = f":extension: {file_type['type']}"
        target_keywords = [""]
        if file_type["keywords"] == "keywords":
            target_keywords = keywords
        # Go through all keywords one by one as query length for Figshare is limited.
        found_datasets_per_filetype = set()
        for keyword in target_keywords:
            if keyword:
                query = (
                    f"{base_query} AND (:title: '{keyword}' "
                    f"OR :description: '{keyword}' OR :keyword: '{keyword}')"
                )
            else:
                query = base_query
            ctx.logger.info("Search query:")
            ctx.logger.info(query)
            page = 1
            found_datasets_per_keyword = []
            # Search endpoint:
            # https://docs.figshare.com/#articles_search
            # Iterate seach on pages.
            while True:
                data_query = {
                    "order": "published_date",
                    "search_for": query,
                    "page": page,
                    "page_size": max_hits_per_page,
                    "order_direction": "desc",
                    "item_type": 3,  # datasets
                }
                results = api.query(endpoint="/articles/search", data=data_query)
                if results["status_code"] >= 400:
                    ctx.logger.warning(
                        f"Failed to fetch page {page} "
                        f"for file extension {file_type['type']}"
                    )
                    ctx.logger.warning(f"Status code: {results['status_code']}")
                    ctx.logger.warning(f"Response headers: {results['headers']}")
                    ctx.logger.warning(f"Response body: {results['response']}")
                    break
                response = results["response"]
                if not response or len(response) == 0:
                    break
                # Extract datasets ids.
                found_datasets_per_keyword_per_page = [hit["id"] for hit in response]
                found_datasets_per_keyword += found_datasets_per_keyword_per_page
                ctx.logger.info(
                    f"Page {page} fetched "
                    f"({len(found_datasets_per_keyword_per_page)} datasets)."
                )
                page += 1
            found_datasets_per_filetype.update(found_datasets_per_keyword)
        ctx.logger.success(
            f"Found {len(found_datasets_per_filetype)} datasets "
            f"for filetype: {file_type['type']}"
        )
        # For debugging purpose, we want unique datasets only, ordered by file types.
        # Instead of a set (sets are unordered),
        # we use a list and remove duplicates later.
        unique_datasets += list(found_datasets_per_filetype)
    # Get unique datasets.
    # This trick preserves the order datasets were found.
    unique_datasets = list(dict.fromkeys(unique_datasets))
    ctx.logger.success(f"Found {len(unique_datasets)} unique datasets.")
    return unique_datasets


def get_metadata_for_datasets(
    api: FigshareAPI, found_datasets: list[str], ctx: toolbox.ContextManager
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get metadata for all selected datasets.

    Parameters
    ----------
    api : FigshareAPI
        Figshare API object.
    found_datasets : list[str]
        List of Figshare dataset ids.
    ctx : toolbox.ContextManager
        ContextManager object.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Dataframes for:
        - datasets
        - files
    """
    datasets_lst = []
    files_lst = []
    # Go through all found datasets and extract information.
    # One dataset at a time.
    datasets_counter = 0
    for dataset_id in found_datasets:
        datasets_counter += 1
        ctx.logger.info(
            f"Fetching metadata for dataset id: {dataset_id} "
            f"({datasets_counter}/{len(found_datasets)})"
        )
        results = api.query(endpoint=f"/articles/{dataset_id}")
        if results["status_code"] >= 400 or results["response"] is None:
            ctx.logger.warning("Failed to fetch dataset.")
            continue
        resp_json_article = results["response"]
        dataset_info, files_info = extract_metadata_from_single_dataset_record(
            resp_json_article
        )
        ctx.logger.info("Done.")
        datasets_lst.append(dataset_info)
        files_lst += files_info
    # Prepare dataframes for export.
    datasets_df = pd.DataFrame(data=datasets_lst)
    files_df = pd.DataFrame(data=files_lst)
    return datasets_df, files_df


def main() -> None:
    """Scrap Figshare datasets and files."""
    # Define data repository name.
    repository_name = "figshare"
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
    # Load API tokens.
    toolbox.load_token()
    # Create API object.
    api = FigshareAPI(
            token=os.getenv("FIGSHARE_TOKEN"),
            logger=context.logger,
        )
    # Test API token validity.
    if api.is_token_valid():
        context.logger.success("Figshare token is valid!")
    else:
        context.logger.error("Figshare token is invalid!")
        context.logger.error("Exiting.")
        sys.exit(1)
    # Search datasets.
    found_datasets = search_all_datasets(api, context)
    # Extract information for all found datasets.
    datasets_df, files_df = get_metadata_for_datasets(api, found_datasets, context)
    context.logger.success(f"Total number of datasets found: {datasets_df.shape[0]}")
    context.logger.success(f"Total number of files found: {files_df.shape[0]}")

    # Add files inside zip archives.
    zip_df = scrap_zip_files(files_df, context.logger)
    context.logger.success(f"Number of files found inside zip files: {zip_df.shape[0]}")
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    context.logger.success(f"Total number of files found: {files_df.shape[0]}")

    # Remove unwanted files based on exclusion lists.
    context.logger.info("Removing unwanted files...")
    _, _, exclude_files, exclude_paths = toolbox.read_query_file(args.query, context.logger)
    files_df = toolbox.remove_excluded_files(files_df, exclude_files, exclude_paths)
    context.logger.info("-" * 30)

    # Remove datasets that contain non-MD related files.
    datasets_df, files_df = toolbox.find_remove_false_positive_datasets(
        datasets_df, files_df, context
    )

    # Save dataframes to disk.
    toolbox.export_dataframe_to_parquet(
        "figshare", toolbox.DataType.DATASETS, datasets_df, context
    )
    toolbox.export_dataframe_to_parquet(
        "figshare", toolbox.DataType.FILES, files_df, context
    )

    # Script duration.
    elapsed_time = int(time.perf_counter() - start_time)
    context.logger.info(f"Scraping Figshare in: {timedelta(seconds=elapsed_time)}")


if __name__ == "__main__":
    main()

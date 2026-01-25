"""Scrape molecular dynamics datasets and files from Figshare."""

import json
import os
import sys
from pathlib import Path

import click
import loguru
import pandas as pd
from dotenv import load_dotenv

from ..core.figshare_api import FigshareAPI
from ..core.logger import create_logger
from ..core.network import get_html_page_with_selenium
from ..core.toolbox import (
    clean_text,
    find_remove_false_positive_datasets,
    make_http_get_request_with_retries,
    print_statistics,
    read_query_file,
    remove_excluded_files,
)
from ..models.enums import DatasetSourceName
from ..models.scraper import ScraperContext
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)


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
    file_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[str]:
    """Extract files from a zip file content.

    No endpoint is available in the Figshare API.
    We perform a direct HTTP GET request to the zip file content url.
    We need to use the Selenium library to emulate a browser request
    as direct requests fail with a 202 status code.

    Known issue with:
    https://figshare.com/ndownloader/files/31660220/preview/31660220/structure.json

    Parameters
    ----------
    file_id : str
        ID of the zip file to get content from.
    logger : "loguru.Logger"
        Logger object.

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
    response = get_html_page_with_selenium(url, tag="pre", logger=logger)
    if response is None:
        logger.warning("Cannot get zip file content.")
        return file_names
    # Extract file names from JSON response.
    try:
        file_names = extract_files_from_json_response(json.loads(response))
    except (json.decoder.JSONDecodeError, ValueError) as exc:
        logger.warning(f"Cannot extract files from HTML response: {exc}")
        logger.debug("Response content:")
        logger.debug(response)
    logger.success(f"Found {len(file_names)} files.")
    return file_names


def get_stats_for_dataset(
    dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> dict:
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
    logger: loguru.Logger
        Logger object.

    Returns
    -------
    dict
        Figshare response as a JSON object.
    """
    stats = {"downloads": None, "views": None}
    for stat in stats:
        response = make_http_get_request_with_retries(
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


def scrap_zip_files_content(
    files_df: pd.DataFrame, logger: "loguru.Logger" = loguru.logger
) -> pd.DataFrame:
    """Scrap information from files contained in zip archives.

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
    for zip_files_counter, zip_idx in enumerate(zip_files_df.index, start=1):
        zip_file = zip_files_df.loc[zip_idx]
        file_id = zip_file["file_url"].split("/")[-1]
        logger.info("Extracting files from zip file:")
        logger.info(zip_file["file_url"])
        file_names = extract_files_from_zip_file(file_id, logger)
        if file_names == []:
            logger.warning("No file found!")
            continue
        # Add other metadata.
        for name in file_names:
            file_metadata = {}
            file_metadata["dataset_repository_name"] = zip_file[
                "dataset_repository_name"
            ]
            file_metadata["dataset_id_in_repository"] = zip_file[
                "dataset_id_in_repository"
            ]
            file_metadata["dataset_url_in_repository"] = zip_file[
                "dataset_url_in_repository"
            ]
            file_metadata["file_name"] = name
            file_metadata["file_url_in_repository"] = zip_file["file_url_in_repository"]
            file_metadata["containing_archive_file_name"] = zip_file["file_name"]
            files_in_zip_lst.append(file_metadata)
        logger.info(
            f"{zip_files_counter} Figshare zip files processed "
            f"({zip_files_counter}/{len(zip_files_df)}"
            f":{zip_files_counter / len(zip_files_df):.0%})"
        )
    files_in_zip_df = pd.DataFrame(files_in_zip_lst)
    logger.success("Done extracting files from zip archives.")
    return files_in_zip_df


def extract_metadata_from_single_dataset_record(
    record_json: dict,
    scraper: ScraperContext,
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
    scraper: ScraperContext
        ScraperContext object.

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
    dataset_stats = {"download_number": None, "view_number": None}
    dataset_info = {
        "dataset_repository_name": scraper.data_source_name,
        "dataset_id_in_repository": dataset_id,
        "dataset_url_in_repository": record_json["url_public_html"],
        "date_created": record_json["created_date"],
        "date_last_updated": record_json["modified_date"],
        "title": clean_text(record_json["title"]),
        "author": clean_text(record_json["authors"][0]["full_name"]),
        "description": clean_text(record_json["description"]),
        "keywords": "",
        "license": record_json["license"]["name"],
        "doi": record_json["doi"],
        "download_number": dataset_stats["download_number"],
        "view_number": dataset_stats["view_number"],
        "number_of_files": len(record_json["files"]),
    }
    # Add keywords only if any.
    if "tags" in record_json:
        dataset_info["keywords"] = ";".join(
            [clean_text(keyword) for keyword in record_json["tags"]]
        )
    for file_in in record_json["files"]:
        file_dict = {
            "dataset_repository_name": dataset_info["dataset_repository_name"],
            "dataset_id_in_repository": dataset_info["dataset_id_in_repository"],
            "dataset_url_in_repository": dataset_info["dataset_url_in_repository"],
            "file_name": file_in["name"],
            "file_url_in_repository": file_in["download_url"],
            "file_size_in_bytes": file_in["size"],
            "file_md5": file_in["computed_md5"],
            "containing_archive_file_name": None,
        }
        files_info.append(file_dict)
    return dataset_info, files_info


def search_all_datasets(
    api: FigshareAPI,
    scraper: ScraperContext,
    max_hits_per_page: int = 100,
    logger: "loguru.Logger" = loguru.logger,
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
    scraper : ScraperContext
        ScraperContext object.
    max_hits_per_page : int
        Maximum number of hits per page.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list of str
        List of Figshare datasets ids.
    """
    # Read parameter file
    file_types, keywords, _, _ = read_query_file(scraper.query_file_path, logger=logger)
    # We use paging to fetch all results.
    # We query max_hits_per_page hits per page.
    unique_datasets = []
    logger.info("-" * 30)
    for file_type in file_types:
        logger.info(f"Looking for filetype: {file_type['type']}")
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
            logger.info("Search query:")
            logger.info(query)
            page = 1
            found_datasets_per_keyword = []
            # Search endpoint: /articles/search
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
                    logger.warning(
                        f"Failed to fetch page {page} "
                        f"for file extension {file_type['type']}"
                    )
                    logger.warning(f"Status code: {results['status_code']}")
                    logger.warning(f"Response headers: {results['headers']}")
                    logger.warning(f"Response body: {results['response']}")
                    break
                response = results["response"]
                if not response or len(response) == 0:
                    break
                # Extract datasets ids.
                found_datasets_per_keyword_per_page = [hit["id"] for hit in response]
                found_datasets_per_keyword += found_datasets_per_keyword_per_page
                logger.info(
                    f"Page {page} fetched "
                    f"({len(found_datasets_per_keyword_per_page)} datasets)."
                )
                page += 1
            found_datasets_per_filetype.update(found_datasets_per_keyword)
        logger.success(
            f"Found {len(found_datasets_per_filetype)} datasets "
            f"for filetype: {file_type['type']}"
        )
        # For debugging purpose, we want unique datasets only,
        # ordered by file types query.
        # Instead of a set (sets are unordered),
        # we use a list and remove duplicates later.
        unique_datasets += list(found_datasets_per_filetype)
    # Get unique datasets.
    # This trick preserves the order datasets were found.
    unique_datasets = list(dict.fromkeys(unique_datasets))
    logger.success(f"Found {len(unique_datasets)} unique datasets.")
    return unique_datasets


def get_metadata_for_datasets_and_files(
    api: FigshareAPI,
    found_datasets: list[str],
    scraper: ScraperContext,
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get metadata for all selected datasets.

    Parameters
    ----------
    api : FigshareAPI
        Figshare API object.
    found_datasets : list[str]
        List of Figshare dataset ids.
    scraper : ScraperContext
        ScraperContext object.
    logger : "loguru.Logger"
        Logger for logging messages.

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
    for datasets_counter, dataset_id in enumerate(found_datasets, start=1):
        logger.info(
            f"Fetching metadata for Figshare dataset id: {dataset_id} "
            f"({datasets_counter}/{len(found_datasets)}"
            f":{datasets_counter / len(found_datasets):.0%})"
        )
        results = api.query(endpoint=f"/articles/{dataset_id}")
        if results["status_code"] >= 400 or results["response"] is None:
            logger.warning("Failed to fetch dataset.")
            continue
        resp_json_article = results["response"]
        dataset_info, files_info = extract_metadata_from_single_dataset_record(
            resp_json_article, scraper
        )
        logger.info("Done.")
        datasets_lst.append(dataset_info)
        files_lst += files_info
    # Convert list to dataframes.
    datasets_df = pd.DataFrame(data=datasets_lst)
    files_df = pd.DataFrame(data=files_lst)
    return datasets_df, files_df


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
@click.option(
    "--query-file",
    "query_file_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Query parameters file (YAML format).",
)
def main(output_dir_path: Path, query_file_path: Path) -> None:
    """Scrape Figshare datasets and files."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.FIGSHARE,
        output_dir_path=output_dir_path,
    )
    logger = create_logger(logpath=scraper.log_file_path, level="INFO")
    # Log script name and doctring.
    logger.info(__file__)
    logger.info(__doc__)
    # Load API tokens.
    load_dotenv()
    # Create API object.
    api = FigshareAPI(
        token=os.getenv("FIGSHARE_TOKEN"),
        logger=logger,
    )
    # Test API token validity.
    if api.is_token_valid():
        logger.success("Figshare token is valid!")
    else:
        logger.error("Figshare token is invalid!")
        logger.error("Exiting.")
        sys.exit(1)
    # Search datasets.
    found_datasets = search_all_datasets(api, scraper, logger=logger)
    # Extract information for all found datasets.
    datasets_df, files_df = get_metadata_for_datasets_and_files(
        api, found_datasets, scraper, logger=logger
    )
    logger.success(f"Total number of datasets found: {datasets_df.shape[0]}")
    logger.success(f"Total number of files found: {files_df.shape[0]}")

    # Add files inside zip archives.
    zip_df = scrap_zip_files_content(files_df, logger)
    logger.success(f"Number of files found inside zip files: {zip_df.shape[0]}")
    files_df = pd.concat([files_df, zip_df], ignore_index=True)
    logger.success(f"Total number of files found: {files_df.shape[0]}")

    # Remove unwanted files based on exclusion lists.
    logger.info("Removing unwanted files...")
    _, _, exclude_files, exclude_paths = read_query_file(query_file_path, logger)
    files_df = remove_excluded_files(files_df, exclude_files, exclude_paths)
    logger.info("-" * 30)

    # Remove datasets that contain non-MD related files.
    datasets_df, files_df = find_remove_false_positive_datasets(
        datasets_df, files_df, scraper, logger=logger
    )

    # Normalize datasets metadata with Pydantic model.
    datasets_metadata = datasets_df.to_dict(orient="records")
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_metadata, logger=logger
    )
    # Normalize files metadata with Pydantic model.
    files_metadata = files_df.to_dict(orient="records")
    files_normalized_metadata = normalize_files_metadata(files_metadata, logger=logger)
    # Save metadata to parquet files.
    export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_normalized_metadata,
        logger=logger,
    )
    export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_normalized_metadata,
        logger=logger,
    )
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()

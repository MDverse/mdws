"""Upload datasets files to Zenodo."""

import argparse
from pathlib import Path
import hashlib
import json
import os
import sys

import dotenv
import numpy as np
import pandas as pd
import requests

import toolbox

ROOT_URL = "https://sandbox.zenodo.org"


def get_cli_arguments():
    """Commande line argument parser.

    Returns
    -------
    argparse.Namespace
        Object containing arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        action="extend",
        type=str,
        nargs="+",
        help="Path to find data files to upload.",
        required=True,
    )
    return parser.parse_args()


def read_zenodo_token():
    """Read Zenodo token from .env file.

    Returns
    -------
    str
        Zenodo token.
    """
    dotenv.load_dotenv(".env")
    if "ZENODO_TOKEN_UPDATE" in os.environ:
        print("Found Zenodo token.")
    else:
        print("Zenodo token is missing.")
        sys.exit(1)
    return os.environ.get("ZENODO_TOKEN_UPDATE", "")


def test_zenodo_connection(root_url="", token="", show_headers=False):
    """Test connection to Zenodo API.

    Zenodo HTTP status codes are listed here:
    https://developers.zenodo.org/#http-status-codes

    Parameters
    ----------
    root_url : str
        Base url for Zenodo API.
    token : str
        Token for Zenodo API
    show_headers : bool
        Default: False
        If true, prints HTTP response headers

    Parameters
    ----------
    token : str
        Token for Zenodo API
    """
    print("Trying connection to Zenodo API...")
    # Basic Zenodo query
    response = requests.get(
        f"{root_url}/api/deposit/depositions",
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


def handle_response(url="", request_response=None, status_code_success=200):
    """Handle API response.

    If needed, show error message and exit.

    Parameters
    ----------
    url : str
        URL of the API request.
    request_response : requests.Response
        Response of the API request.
    status_code_success : int
        Default: 200
        Status code for a successful request.
    """
    if request_response.status_code == status_code_success:
        print("-> success!")
    else:
        print("Error with url:")
        print(url)
        print("Status code:", request_response.status_code)
        print("Headers:", json.dumps(dict(request_response.headers), indent=2))
        print("Content:", json.dumps(request_response.json(), indent=2))
        sys.exit(1)


def get_local_files(file_path_lst):
    """List local files.

    Parameters
    ----------
    file_path_lst : list
        List of local file paths.

    Returns
    -------
    pd.DataFrame
        Pandas dataframe with local files information.
    """
    local_files = []
    for file_path in file_path_lst:
        toolbox.verify_file_exists(file_path)
        local_files.append(
            {
                "name": Path(file_path).name,
                "path": file_path,
                "md5sum": hashlib.md5(open(file_path, "rb").read()).hexdigest(),
            }
        )
    return pd.DataFrame(local_files)


def find_last_version_id(root_url="", record_id="", token=""):
    """Find last version of a record.

    Parameters
    ----------
    root_url : str
        Base url for Zenodo API, by default "".
    record_id : str
        Record ID belonging to the same record serie, by default "".
    token : str
        Zenodo API token, by default "".

    Returns
    -------
    str
        ID of the last version of the record.
    """
    print("Finding last version of the record")
    url = f"{root_url}/api/records/{record_id}"
    params = {"access_token": token}
    response = requests.get(url, params=params)
    handle_response(url=url, request_response=response, status_code_success=200)
    last_record_url = response.json()["links"]["latest"]
    print(f"Last record URL: {last_record_url}")
    return last_record_url.split("/")[-1]


def update_metadata(root_url="", record_id="", metadata="", token=""):
    """Update record metadata.

    Doc: https://developers.zenodo.org/#update
    Parameters
    ----------
    root_url : str
        Base url for Zenodo API, by default "".
    record_id : str
        Record ID, by default "".
    metadata : str
        Metadata of Zenodo record, by default "".
    token : str
        Zenodo API token, by default "".
    """
    print(f"Updating metadata for record {record_id}")
    url = f"{root_url}/api/deposit/depositions/{record_id}?access_token={token}"
    params = {"access_token": token}
    headers = {"Content-Type": "application/json"}
    response = requests.put(url, data=json.dumps(metadata), headers=headers)
    handle_response(url=url, request_response=response, status_code_success=200)


def list_files_in_record(root_url="", record_id="", token=""):
    """List files in a record.

    Parameters
    ----------
    root_url : str
        Base url for Zenodo API, by default "".
    record_id : str
        Record ID, by default "".
    token : str
        Zenodo API token, by default "".

    Returns
    -------
    pd.DataFrame
        Pandas dataframe with files information.
    """
    print(f"Listing files in record {record_id}")
    # https://developers.zenodo.org/#list23
    url = f"{root_url}/api/deposit/depositions/{record_id}/files"
    params = {"access_token": token}
    response = requests.get(url, params=params)
    handle_response(url=url, request_response=response, status_code_success=200)
    zenodo_files = response.json()
    for zenodo_file_dict in zenodo_files:
        print(
            f"Found in Zenodo: file {zenodo_file_dict['filename']} with ID {zenodo_file_dict['id']}"
        )
    zenodo_files_df = pd.DataFrame(zenodo_files).rename(columns={"filename": "name"})
    return zenodo_files_df


def check_file_status(local_files=pd.DataFrame(), zenodo_files=pd.DataFrame()):
    """Check status of files.

    Parameters
    ----------
    local_files : pd.DataFrame
        Pandas dataframe with local files information.
    zenodo_files : pd.DataFrame
        Pandas dataframe with Zenodo files information.

    Returns
    -------
    pd.DataFrame
        Pandas dataframe with files information and status.
    """
    print("Checking files status")
    files = (
        pd.merge(local_files, zenodo_files, how="outer", on="name")
        .loc[:, ["name", "path", "md5sum", "checksum", "id"]]
        .fillna(value="")
    )
    # files_df = files_df[["name", "path", "md5sum", "checksum", "id"]]
    # status could be:
    # "keep": file is already in Zenodo and has not changed.
    # "update": file is already in Zenodo and has changed.
    # "delete": file is in Zenodo but not locally.
    # "add": file is not in Zenodo but locally.
    files["status"] = ""
    for index, row in files.iterrows():
        if row["md5sum"] == row["checksum"]:
            files.at[index, "status"] = "keep"
        if (row["md5sum"] != row["checksum"]) and (row["id"]):
            files.at[index, "status"] = "update"
        if (row["md5sum"] != row["checksum"]) and (not row["id"]):
            files.at[index, "status"] = "add"
        if not row["path"]:
            files.at[index, "status"] = "delete"
        print(f"{row['name']} -> {row['status']}")
    if (files["status"] == "keep").all():
        print("No files to update. Exiting.")
        sys.exit()
    return files


def create_new_version(root_url="", record_id="", token=""):
    """Create new version of a record.

    Parameters
    ----------
    root_url : str
        Base url for Zenodo API, by default "".
    record_id : str
        Record ID, by default "".
    token : str
        Zenodo API tojen, by default "".

    Returns
    -------
    str
        URL of the new version of the record.
    """
    print("Creating new version")
    url = f"{root_url}/api/deposit/depositions/{record_id}/actions/newversion"
    params = {"access_token": token}
    response = requests.post(url, params=params)
    handle_response(url=url, request_response=response, status_code_success=201)
    return response.json()["links"]["latest_draft"]


def delete_files(deposition_url="", files=pd.DataFrame(), token=""):
    """Delete files with status "update" or "delete".

    Doc: https://developers.zenodo.org/#delete28

    Parameters
    ----------
    deposition_url : str
        Record URL for deposition, by default "".
    files : pd.DataFrame
        Dataframe with files information, by default pd.DataFrame().
    token : str
        Zenodo API token, by default "".
    """
    for index, row in files.iterrows():
        if row["status"] in ["update", "delete"]:
            print(f"Deleting file {row['name']}")
            url = f"{deposition_url}/files/{row['id']}"
            params = {"access_token": token}
            response = requests.delete(url, params=params)
            handle_response(url=url, request_response=response, status_code_success=204)


def add_files(deposition_url="", files=pd.DataFrame(), token=""):
    """Add files with status "add" or "update".

    Doc: https://developers.zenodo.org/#create24

    Parameters
    ----------
    deposition_url : str
        Record URL for deposition, by default "".
    files : pd.DataFrame
        Dataframe with files information, by default pd.DataFrame().
    token : str
        Zenodo API token, by default "".
    """
    # https://developers.zenodo.org/#create24
    # Add files with status "add" or "update"
    for index, row in files.iterrows():
        if row["status"] in ["update", "add"]:
            print(f"Adding file {row['name']}")
            url = f"{deposition_url}/files"
            data = {"access_token": token, "name": row["name"]}
            file_content = {"file": open(row["path"], "rb")}
            response = requests.post(url, data=data, files=file_content)
            handle_response(url=url, request_response=response, status_code_success=201)


def publish_new_version(deposition_url="", token=""):
    """Publish new version of the record.

    Doc: https://developers.zenodo.org/#publish

    Parameters
    ----------
    deposition_url : str
        Record URL for deposition, by default "".
    token : str
        Zenodo API token, by default "".
    """
    print("Publishing new version")
    url = f"{deposition_url}/actions/publish"
    params = {"access_token": token}
    response = requests.post(url, params=params)
    handle_response(url=url, request_response=response, status_code_success=202)
    print(f"Latest version published at: {response.json()['links']['latest_html']}")


if __name__ == "__main__":
    ARGS = get_cli_arguments()

    # Get md5sums and name for local files
    local_files_df = get_local_files(ARGS.input)

    # Read Zenodo token and test connection
    ZENODO_TOKEN = read_zenodo_token()
    test_zenodo_connection(root_url=ROOT_URL, token=ZENODO_TOKEN)

    # Find latest version of the record
    # https://sandbox.zenodo.org/record/1165558 is the first version of the record
    last_version_id = find_last_version_id(
        root_url=ROOT_URL, record_id="1165558", token=ZENODO_TOKEN
    )

    # Update metadata
    metadata = json.load(open("params/zenodo_share_metadata.json", "rb"))
    # update_metadata(root_url=ROOT_URL, record_id=last_version_id, metadata=metadata, token=ZENODO_TOKEN)

    # List files in record
    zenodo_files_df = list_files_in_record(
        root_url=ROOT_URL, record_id=last_version_id, token=ZENODO_TOKEN
    )

    # Check files status
    files_df = check_file_status(
        local_files=local_files_df, zenodo_files=zenodo_files_df
    )

    # Create a new version of the record
    new_deposition_url = create_new_version(
        root_url=ROOT_URL, record_id=last_version_id, token=ZENODO_TOKEN
    )

    # Delete files
    delete_files(deposition_url=new_deposition_url, files=files_df, token=ZENODO_TOKEN)

    # Add files (status "add" or "update")
    add_files(deposition_url=new_deposition_url, files=files_df, token=ZENODO_TOKEN)

    # Publish new version of the record
    publish_new_version(deposition_url=new_deposition_url, token=ZENODO_TOKEN)

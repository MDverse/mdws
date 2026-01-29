"""Tests for the network module."""

import json

import httpx
import pytest

import mdverse_scrapers.core.network as network
import mdverse_scrapers.core.toolbox as toolbox
from mdverse_scrapers.core.logger import create_logger


@pytest.mark.network
def test_make_http_get_request_with_retries_200():
    """Test the make_http_get_request_with_retries function."""
    url = "https://httpbin.org/get"
    response = toolbox.make_http_get_request_with_retries(
        url=url,
        timeout=5,
        delay_before_request=1,
        max_attempts=1,
    )
    assert response is not None
    assert response.status_code == 200


@pytest.mark.network
def test_make_http_get_request_with_retries_202():
    """Test the make_http_get_request_with_retries function."""
    url = "https://httpbin.org/status/202"
    response = toolbox.make_http_get_request_with_retries(
        url=url,
        timeout=5,
        delay_before_request=1,
        max_attempts=1,
    )
    assert response is None


@pytest.mark.network
def test_make_http_get_request_with_retries_404():
    """Test the make_http_get_request_with_retries function."""
    url = "https://httpbin.org/status/404"
    response = toolbox.make_http_get_request_with_retries(
        url=url,
        timeout=5,
        delay_before_request=1,
        max_attempts=1,
    )
    assert response is None


@pytest.mark.network
def test_get_html_page_with_selenium_good_url():
    """Test the get_html_page_with_selenium function with a good URL."""
    url = "https://figshare.com/ndownloader/files/21988230/preview/21988230/structure.json"
    expected_json = {
        "files": [],
        "path": "ROOT",
        "dirs": [
            {
                "files": [
                    {"path": "NIPAM-FF1.3x/NIPAM-64-wat-ch-1.3.top"},
                    {"path": "NIPAM-FF1.3x/NIPAM-64-wat.gro"},
                    {"path": "NIPAM-FF1.3x/md.mdp"},
                    {"path": "NIPAM-FF1.3x/NIPAM-ch-1.3.itp"},
                ],
                "path": "NIPAM-FF1.3x",
                "dirs": [],
            }
        ],
    }
    content = network.get_html_page_with_selenium(url=url, tag="pre")
    if not content:
        pytest.fail("Failed to retrieve content from the URL.")
    assert json.loads(content) == expected_json


@pytest.mark.network
def test_get_html_page_with_selenium_bad_url(capsys) -> None:
    """Test the get_html_page_with_selenium function with a bad URL."""
    url = "https://figshare.com/ndownloader/files/28089615/preview/28089615/structure.json"
    content = network.get_html_page_with_selenium(
        url=url, tag="pre", logger=create_logger(level="DEBUG")
    )
    assert content is None
    captured = capsys.readouterr()
    assert "Timeout while retrieving page" in captured.out


@pytest.mark.parametrize(
    ("file_url", "expected_size"),
    [
        ("https://httpbin.org/bytes/1024", 1024),
        ("https://httpbin.org/status/404", None),
        ("https://www.gpcrmd.org//dynadb/files/Dynamics/10192_prm_12.tar.gz", 148841),
        ("https://www.gpcrmd.org//dynadb/files/Dynamics/10205_dyn_13.pdb", 6984955),
        ("https://www.gpcrmd.org//dynadb/files/Dynamics/10201_trj_13.xtc", 799818836),
        ("https://www.gpcrmd.org//dynadb/files/Dynamics/non_existent_file.txt", None),
    ],
)
@pytest.mark.network
def test_retrieve_file_size_from_http_head_request(file_url, expected_size) -> None:
    """Test the retrieve_file_size_from_http_head_request function."""
    with httpx.Client() as client:
        file_size = network.retrieve_file_size_from_http_head_request(
            client=client,
            url=file_url,
        )
    assert file_size == expected_size

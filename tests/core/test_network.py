"""Tests for the network module."""

import json

import mdverse_scrapers.core.network as network
import mdverse_scrapers.core.toolbox as toolbox
from mdverse_scrapers.core.logger import create_logger


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


def test_get_html_page_with_selenium_good_url():
    """Test the get_html_page_with_selenium function with a bad URL."""
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
    assert json.loads(content) == expected_json


def test_get_html_page_with_selenium_bad_url(capsys) -> None:
    """Test the get_html_page_with_selenium function with a bad URL."""
    url = "https://figshare.com/ndownloader/files/28089615/preview/28089615/structure.json"
    content = network.get_html_page_with_selenium(
        url=url, tag="pre", logger=create_logger(level="DEBUG")
    )
    assert content is None
    captured = capsys.readouterr()
    assert "Timeout while retrieving page" in captured.out

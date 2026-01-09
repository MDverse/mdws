import pytest

import mdverse_web_scraper.core.toolbox as toolbox


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

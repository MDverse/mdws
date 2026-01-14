"""Common functions and network utilities."""

import time
from enum import StrEnum

import httpx
import loguru


class HttpMethod(StrEnum):
    """HTTP methods."""

    GET = "GET"
    POST = "POST"
    HEAD = "HEAD"


def create_httpx_client(
    base_url: str = "",
) -> httpx.Client:
    """Create an HTTPX client with custom settings.

    https://www.python-httpx.org/advanced/clients/

    Parameters
    ----------
    base_url : str
        Base URL for the HTTP client.

    Returns
    -------
    httpx.Client
        Configured HTTPX client.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        ),
    }
    return httpx.Client(base_url=base_url, headers=headers)


def make_http_request_with_retries(
    client: httpx.Client,
    url: str,
    method: HttpMethod = HttpMethod.GET,
    headers: dict | None = None,
    params: dict | None = None,
    data: dict | None = None,
    json: dict | None = None,
    timeout: int = 10,
    delay_before_request: int = 1,
    max_attempts: int = 3,
    logger: "loguru.Logger" = loguru.logger,
) -> httpx.Response | None:
    """Make HTTP request with retries on failure.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL to send the request to.
    method : HttpMethod
        The HTTP method to use for the request.
    headers : dict | None
        Dictionary of headers to include in the request.
    params : dict | None
        Dictionary of query parameters to include in the request URL.
        Used with GET requests.
    data : dict | None
        Dictionary of form data to include in the request body.
        Used with POST requests.
    json: dict | None
        Dictionary of JSON data to include in the request body.
        Used with POST requests.
    timeout : int
        Timeout for the HTTP request in seconds.
    max_attempts : int
        Maximum number of attempts to make.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    httpx.Response | None
        The HTTP response if successful, None otherwise.

    Raises
    ------
    httpx.HTTPStatusError
        If the request returns a 202 code,
        indicating the request is accepted but not ready yet.
        This error is caught and retried.
    """
    logger.info(f"Making HTTP {method} request to:")
    logger.info(url)
    for attempt in range(1, max_attempts + 1):
        try:
            # Fist attempt, wait delay_before_request seconds,
            # Second attempt, wait delay_before_request + 10 seconds,
            # Third attempt, wait delay_before_request + 20 seconds, etc.
            time.sleep(delay_before_request + (attempt - 1) * 10)
            response = client.request(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                json=json,
                follow_redirects=True,
                timeout=timeout,
            )
            response.raise_for_status()
            # Raise an error if status code is 202,
            # indicating the request is accepted but not ready yet.
            if response.status_code == 202:
                msg = "Status code 202. Request accepted but not ready yet."
                raise httpx.HTTPStatusError(
                    msg, request=response.request, response=response
                )
            return response
        except httpx.TimeoutException:
            logger.warning(f"Attempt {attempt}/{max_attempts} timed out.")
            logger.warning(f"Timeout: {timeout} seconds.")
        except httpx.RequestError as exc:
            # httpx.RequestError only has a .request attribute.
            logger.warning(f"Attempt {attempt}/{max_attempts} failed.")
            logger.debug("Query headers:")
            logger.debug(exc.request.headers)
            logger.warning(f"Error details: {exc}")
        except httpx.HTTPStatusError as exc:
            # httpx.HTTPStatusError has .request and .response attributes.
            logger.warning(f"Attempt {attempt}/{max_attempts} failed.")
            logger.warning(f"Status code: {exc.response.status_code}")
            logger.debug("Query headers:")
            logger.debug(exc.request.headers)
            logger.debug("Response headers:")
            logger.debug(exc.response.headers)
        if attempt == max_attempts:
            logger.error(f"Maximum attempts ({max_attempts}) reached for URL:")
            logger.error(url)
            logger.error("Giving up!")
        else:
            logger.info("Retrying...")
    return None

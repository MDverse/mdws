"""Common functions and network utilities."""

import json
import time
from enum import StrEnum
from io import BytesIO

import certifi
import httpx
import loguru
import pycurl
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait


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


def is_connection_to_server_working(
    client: httpx.Client, url: str, logger: "loguru.Logger" = loguru.logger
) -> bool | None:
    """Test connection to a web server.

    Parameters
    ----------
    client : httpx.Client
        The HTTPX client to use for making requests.
    url : str
        The URL endpoint.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.
    """
    logger.debug("Testing connection to server...")
    response = make_http_request_with_retries(
        client, url, method=HttpMethod.GET, max_attempts=2, logger=logger
    )
    if not response:
        logger.error("Cannot connect to server.")
        return False
    if response and hasattr(response, "headers"):
        logger.debug(response.headers)
    return True


def make_http_request_with_retries(
    client: httpx.Client,
    url: str,
    method: HttpMethod = HttpMethod.GET,
    headers: dict | None = None,
    params: dict | None = None,
    data: dict | None = None,
    json: dict | None = None,
    timeout: int = 10,
    delay_before_request: float = 1.0,
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
    logger.info(f"Sending HTTP {method} request to:")
    logger.info(url)
    for attempt in range(1, max_attempts + 1):
        try:
            # First attempt, wait delay_before_request seconds,
            # Second attempt, wait delay_before_request + 10 seconds,
            # Third attempt, wait delay_before_request + 20 seconds, etc.
            wait_time = delay_before_request + (attempt - 1) * 10
            if attempt > 1:
                logger.info(f"Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
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


def parse_response_headers(headers_bytes: bytes) -> dict[str, str]:
    """Parse HTTP response header from bytes to a dictionary.

    Returns
    -------
    dict
        A dictionary of HTTP response headers.
    """
    headers = {}
    headers_text = headers_bytes.decode("utf-8")
    for line in headers_text.split("\r\n"):
        if ": " in line:
            key, value = line.split(": ", maxsplit=1)
            headers[key] = value
    return headers


def send_http_request_with_retries_pycurl(
    url: str,
    data: dict | None = None,
    delay_before_request: float = 1.0,
    logger: "loguru.Logger" = loguru.logger,
) -> dict:
    """Query the Figshare API and return the JSON response.

    Parameters
    ----------
    url : str
        URL to send the request to.
    data : dict, optional
        Data to send in the request body (for POST requests).
    delay_before_request : float, optional
        Time to wait before sending the request, in seconds.

    Returns
    -------
    dict
        A dictionary with the following keys:
        - status_code: HTTP status code of the response.
        - elapsed_time: Time taken to perform the request.
        - headers: Dictionary of response headers.
        - response: JSON response from the API.
    """
    # First, we wait.
    # https://docs.figshare.com/#figshare_documentation_api_description_rate_limiting
    # "We recommend that clients use the API responsibly
    # and do not make more than one request per second."
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
    }
    time.sleep(delay_before_request)
    results = {}
    # Initialize a Curl object.
    curl = pycurl.Curl()
    # Set the URL to send the request to.
    curl.setopt(curl.URL, url)
    # Add headers as a list of strings.
    headers_lst = [f"{key}: {value}" for key, value in headers.items()]
    curl.setopt(curl.HTTPHEADER, headers_lst)
    # Handle SSL certificates.
    curl.setopt(curl.CAINFO, certifi.where())
    # Follow redirect.
    curl.setopt(curl.FOLLOWLOCATION, True)  # noqa: FBT003
    # If data is provided, set the request to POST and add the data.
    if data is not None:
        curl.setopt(curl.POST, True)  # noqa: FBT003
        data_json = json.dumps(data)
        curl.setopt(curl.POSTFIELDS, data_json)
    # Capture the response body in a buffer.
    body_buffer = BytesIO()
    curl.setopt(curl.WRITEFUNCTION, body_buffer.write)
    # Capture the response headers in a buffer.
    header_buffer = BytesIO()
    curl.setopt(curl.HEADERFUNCTION, header_buffer.write)
    # Perform the request.
    curl.perform()
    # Get the HTTP status code.
    status_code = curl.getinfo(curl.RESPONSE_CODE)
    results["status_code"] = status_code
    # Get elapsed time.
    elapsed_time = curl.getinfo(curl.TOTAL_TIME)
    results["elapsed_time"] = elapsed_time
    # Close the Curl object.
    curl.close()
    # Get the response headers from the buffer.
    response_headers = parse_response_headers(header_buffer.getvalue())
    results["headers"] = response_headers
    # Get the response body from the buffer.
    response = body_buffer.getvalue()
    # Convert the response body from bytes to a string.
    response = response.decode("utf-8")
    # Convert the response string to a JSON object.
    try:
        response = json.loads(response)
    except json.JSONDecodeError:
        logger.error("Error decoding JSON response:")
        logger.error(response[:100])
        response = None
    results["response"] = response
    return results


def get_html_page_with_selenium(
    url: str, tag: str = "body", logger: "loguru.Logger" = loguru.logger
) -> str | None:
    """Get HTML page content using Selenium.

    Parameters
    ----------
    url : str
        URL of the web page to retrieve.
    tag : str, optional
        HTML tag to wait for before retrieving the page content (default is "body").

    Returns
    -------
    str | None
        HTML content of the page, or None if an error occurs.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--enable-javascript")
    page_content = ""
    logger.info("Retrieving page with Selenium:")
    logger.info(url)
    try:
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        page_content = (
            WebDriverWait(driver, 5)
            .until(ec.visibility_of_element_located((By.CSS_SELECTOR, tag)))
            .text
        )
        driver.quit()
    except TimeoutException:
        logger.error("Timeout while retrieving page:")
        logger.error(url)
    except WebDriverException as e:
        logger.error("Cannot retrieve page:")
        logger.error(url)
        logger.debug(f"Selenium error: {e}")
        return None
    if not page_content:
        logger.error("Retrieved page content is empty.")
        return None
    return page_content

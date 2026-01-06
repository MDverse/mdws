"""Query the Figshare API using pycurl."""

import json
from io import BytesIO
import time

import certifi
import pycurl


class FigshareAPI:
    """Class to interact with the Figshare API."""

    def __init__(self, token, base_url: str = "https://api.figshare.com/v2/"):
        self.token = token
        self.base_url = base_url
        self.headers = {
            "Authorization": f"token {token}",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
        }

    @staticmethod
    def parse_response_headers(headers_bytes: bytes) -> dict:
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

    def query(self, endpoint: str, data: dict = None) -> dict:
        """Query the Figshare API and return the JSON response.

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
        time.sleep(1)
        results = {}
        # Initialize a Curl object.
        curl = pycurl.Curl()
        # Set the URL to send the request to.
        url = f"{self.base_url}{endpoint}"
        curl.setopt(curl.URL, url)
        # Add headers as a list of strings.
        headers_lst = [f"{key}: {value}" for key, value in self.headers.items()]
        curl.setopt(curl.HTTPHEADER, headers_lst)
        # Handle SSL certificates.
        curl.setopt(curl.CAINFO, certifi.where())
        # Follow redirect.
        curl.setopt(curl.FOLLOWLOCATION, True)
        # If data is provided, set the request to POST and add the data.
        if data is not None:
            curl.setopt(curl.POST, True)
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
        response_headers = self.parse_response_headers(header_buffer.getvalue())
        results["headers"] = response_headers
        # Get the response body from the buffer.
        response = body_buffer.getvalue()
        # Convert the response body from bytes to a string.
        response = response.decode("utf-8")
        # Convert the response string to a JSON object.
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            print("Error decoding JSON response:")
            print(response[:100])
            response = None
        results["response"] = response
        return results

    def is_token_valid(self) -> bool:
        """Verify if the provided token is valid.

        Returns
        -------
            bool: True if the token is valid, False otherwise.
        """
        response = self.query(endpoint="/token")
        return response["status_code"] == 200


if __name__ == "__main__":
    api = FigshareAPI(
        token="a10511e3d8c648afbeb7b700152241d8be86c7f9a93c1a68b068b62cffd519f3f0d60caf45c13baf10114e40ea95165f9454c5a5de4402c08344b6340f2af2e8",
        base_url="https://api.figshare.com/v2/",
    )

    if api.is_token_valid():
        print("API token is valid.")

    response = api.query(
        endpoint="/token",
    )
    print(response)

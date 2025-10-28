"""Scrap molecular dynamics datasets and files from NOMAD

Usage :
=======
    uv run scripts/scrap_nomad.py
"""


import json
import pathlib
import time
import numpy as np
import pandas as pd
from datetime import datetime


import httpx
from loguru import logger



if __name__ == "__main__":
    logger.add("logs/scrap_nomad_{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days")

    start_time = time.time()
    logger.info("Starting Nomad data scraping...")

    # Define output directory
    output_dir = pathlib.Path("data/nomad")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define the URL for Nomad API
    nomad_api_url = "http://nomad-lab.eu/prod/v1/api/v1"

    # Fetch data from Nomad API
    try:
        response = httpx.get(nomad_api_url)
        response.raise_for_status()
        jobs_data = response.json()
        logger.info(f"Fetched {len(jobs_data)} jobs from Nomad API.")
    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        jobs_data = []

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.success(f"Completed Nomad data scraping in {elapsed_time:.2f} seconds.")
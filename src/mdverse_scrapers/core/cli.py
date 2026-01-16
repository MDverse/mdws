"""Utilities to parse command line arguments for scraper scripts."""

import sys
from pathlib import Path

import click


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
def get_cli_output_dir(output_dir: Path):
    """Parse scraper scripts command line.

    Returns
    -------
    output_dir : Path
        The output directory.
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
@click.option(
    "--query-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Query parameters file (YAML format).",
)
def get_cli_output_dir_query_file(output_dir: Path, query_file: Path):
    """Parse scraper scripts command line.

    Returns
    -------
    output_dir : Path
        The output directory path.
    query_file : Path
        The query parameters file path.

    Raises
    ------
    FileNotFoundError
        If the query parameters file does not exist.
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    if not query_file.exists():
        message = f"Query parameters file not found: {query_file}"
        raise FileNotFoundError(message)
        sys.exit(1)
    return output_dir, query_file

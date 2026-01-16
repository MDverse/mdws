"""Utilities to parse command line arguments for scraper scripts."""

from pathlib import Path

import click


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
def get_cli_output_dir(output_dir_path: Path):
    """Parse scraper scripts command line."""
    # Create output directory if it does not exist.
    output_dir_path.mkdir(parents=True, exist_ok=True)


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
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
def get_cli_output_dir_query_file(output_dir_path: Path, query_file: Path):
    """Parse scraper scripts command line.

    Returns
    -------
    output_dir_path : Path
        The output directory path.
    query_file : Path
        The query parameters file path.
    """
    # Create output directory if it does not exist.
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir_path, query_file

"""Tests for the Pydantic scraper model."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.enums import DatasetSourceName
from mdverse_scrapers.models.scraper import ScraperContext


def test_context_paths_are_created(tmp_path):
    """Test default fields are correctly created.

    tmp_path is a pytest fixture that provides a temporary directory.
    """
    base_output = tmp_path / "output"
    base_output.mkdir()

    scraper = ScraperContext(
        data_source_name=DatasetSourceName.ZENODO,
        output_dir_path=base_output,
        start_time=datetime(2026, 1, 1, 12, 34, 56),
    )

    expected_output_dir_path = base_output / "zenodo" / "2026-01-01"
    assert scraper.output_dir_path == expected_output_dir_path
    assert expected_output_dir_path.is_dir()
    assert scraper.log_file_path == expected_output_dir_path / "zenodo_scraper.log"
    assert (
        scraper.datasets_parquet_file_path
        == expected_output_dir_path / "zenodo_datasets.parquet"
    )
    assert (
        scraper.files_parquet_file_path
        == expected_output_dir_path / "zenodo_files.parquet"
    )
    assert scraper.number_of_datasets_scraped == 0
    assert scraper.number_of_files_scraped == 0
    assert scraper.token is None
    assert scraper.query_file_path is None


def test_accepts_existing_query_file(tmp_path):
    """Test the model accepts an existing query file.

    tmp_path is a pytest fixture that provides a temporary directory.
    """
    base_output = tmp_path / "output"
    base_output.mkdir()
    query_file = tmp_path / "query.yml"
    query_file.write_text("dummy: value")
    context = ScraperContext(
        data_source_name=DatasetSourceName.FIGSHARE,
        output_dir_path=base_output,
        query_file_path=query_file,
        start_time=datetime(2026, 1, 1, 12, 34, 56),
    )

    expected_output_dir_path = base_output / "figshare" / "2026-01-01"
    assert context.output_dir_path == expected_output_dir_path
    assert context.query_file_path == query_file


def test_missing_output_directory(tmp_path):
    """Test the model raises a ValidationError when the output directory does not exist.

    tmp_path is a pytest fixture that provides a temporary directory.
    """
    missing_dir = tmp_path / "missing"
    with pytest.raises(ValidationError):
        ScraperContext(
            data_source_name=DatasetSourceName.ZENODO,
            output_dir_path=missing_dir,
        )


def test_missing_param_file(tmp_path):
    """Test the model raises a ValidationError when the parameter file does not exist.

    tmp_path is a pytest fixture that provides a temporary directory.
    """
    base_output = tmp_path / "output"
    base_output.mkdir()
    query_file = tmp_path / "query.yml"
    with pytest.raises(ValidationError):
        ScraperContext(
            data_source_name=DatasetSourceName.ZENODO,
            output_dir_path=base_output,
            query_file_path=query_file,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("number_of_datasets_scraped", -1),
        ("number_of_files_scraped", -5),
    ],
)
def test_numbers_must_be_non_negative(field_name, value, tmp_path):
    """Test that number of datasets and numbers of files scraped must be non-negative.

    tmp_path is a pytest fixture that provides a temporary directory.
    """
    base_output = tmp_path / "output"
    base_output.mkdir()

    with pytest.raises(ValidationError):
        ScraperContext(
            data_source_name=DatasetSourceName.ZENODO,
            output_dir_path=base_output,
            **{field_name: value},
        )

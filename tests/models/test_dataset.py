"""Tests for the pydantic dataset model module."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.dataset import DatasetMetadata
from mdverse_scrapers.models.date import DATETIME_FORMAT
from mdverse_scrapers.models.enums import DatasetSourceName


# --------------------------------------
# Basic instantiation of DatasetMetadata
# --------------------------------------
def test_dataset_metadata_minimal_required_fields():
    """Test creating DatasetMetadata with only required fields."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
    )
    assert metadata.dataset_repository_name == DatasetSourceName.ZENODO
    assert metadata.dataset_id_in_repository == "123"
    assert metadata.dataset_url_in_repository == "https://zenodo.org/record/123"
    assert metadata.title == "Test Dataset"


# ----------------------------------
# Test validators (1) empty_to_none
# ----------------------------------
@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("description", "", None),
        ("keywords", [], None),
        (
            "external_links",
            ["https://doi.org/10.1234/abc"],
            ["https://doi.org/10.1234/abc"],
        ),
    ],
)
def test_empty_to_none(field, value, expected):
    """Test that empty strings/lists are converted to None."""
    data = {
        "dataset_repository_name": DatasetSourceName.ZENODO,
        "dataset_id_in_repository": "123",
        "dataset_url_in_repository": "https://zenodo.org/record/123",
        "title": "Test Dataset",
        field: value,
    }
    metadata = DatasetMetadata.model_validate(data)
    assert getattr(metadata, field) == expected


# ---------------------------------
# Test validators (2) format_dates
# ---------------------------------
def test_format_dates_with_datetime_objects():
    """Test that datetime objects are correctly converted to ISO string format."""
    now = datetime(2026, 1, 23, 12, 0, 0)
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_created=now,
        date_last_updated=now,
    )
    formatted = now.strftime(DATETIME_FORMAT)
    assert metadata.date_created == formatted
    assert metadata.date_last_updated == formatted


def test_format_dates_with_iso_strings():
    """Test that ISO string dates are normalized to '%Y-%m-%dT%H:%M:%S' format."""
    expected_date = "2026-01-23T00:00:00"
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_created="2026-01-23",
    )
    assert metadata.date_created == expected_date


# ------------------------------------------
# Test validators (3) Project field fallback
# ------------------------------------------
def test_fill_project_fields():
    """Test that project fields are populated from repository fields when missing."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="repo_123",
        dataset_url_in_repository="https://zenodo.org/record/repo_123",
        dataset_project_name=None,
        dataset_id_in_project=None,
        dataset_url_in_project=None,
        title="Test Dataset",
    )
    assert metadata.dataset_project_name == DatasetSourceName.ZENODO
    assert metadata.dataset_id_in_project == "repo_123"
    assert metadata.dataset_url_in_project == "https://zenodo.org/record/repo_123"


def test_fill_project_fields_from_repository_invalid_mapping():
    """Test that ValueError is raised when repository cannot map to a project."""
    with pytest.raises(ValidationError):
        _ = DatasetMetadata(
            dataset_repository_name="dummy",
            dataset_id_in_repository="123",
            dataset_url_in_repository="https://example.com/123",
            title="Test Dataset",
        )


def test_date_last_fetched_is_recent():
    """Test that date_last_fetched is a recent datetime."""
    time_1 = datetime.now()
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
    )
    time_2 = datetime.fromisoformat(metadata.date_last_fetched)
    diff = abs((time_2 - time_1).total_seconds())
    assert 0 <= diff <= 2  # Allow up to 2 seconds difference.


# ---------------------
# Full integration test
# ---------------------
def test_dataset_metadata_full_scenario():
    """Test a realistic scenario with mixed missing fields and validators."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetSourceName.FIGSHARE,
        dataset_id_in_repository="fig_456",
        dataset_url_in_repository="https://figshare.com/articles/fig_456",
        dataset_project_name=None,
        dataset_id_in_project=None,
        dataset_url_in_project=None,
        title="Full Test Dataset",
        description="",
        keywords=[],
        author_names=[],
        external_links=[],
        license="",
    )
    # Check that empty fields are converted to None.
    assert metadata.description is None
    assert metadata.keywords is None
    assert metadata.author_names is None
    assert metadata.external_links is None
    assert metadata.license is None
    # Check that project fields are filled
    assert metadata.dataset_project_name == DatasetSourceName.FIGSHARE
    assert metadata.dataset_id_in_project == "fig_456"
    assert metadata.dataset_url_in_project == "https://figshare.com/articles/fig_456"
    # Check that date_last_fetched is filled
    assert isinstance(metadata.date_last_fetched, str)

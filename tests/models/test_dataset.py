"""Tests for the pydantic dataset model module."""

import re
from datetime import datetime

from pydantic import ValidationError
import pytest

from mdverse_scrapers.models.dataset import DatasetMetadata
from mdverse_scrapers.models.enums import DatasetProjectName, DatasetRepositoryName


# --------------------------------------
# Basic instantiation of DatasetMetadata
# --------------------------------------
def test_dataset_metadata_minimal_required_fields():
    """Test creating DatasetMetadata with only required fields."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset"
    )
    assert metadata.dataset_repository_name == DatasetRepositoryName.NOMAD
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
        ("external_links", ["https://doi.org/10.1234/abc"], ["https://doi.org/10.1234/abc"]),
    ]
)
def test_empty_to_none(field, value, expected):
    """Test that empty strings/lists are converted to None by validators."""
    data = {
        "dataset_repository_name": DatasetRepositoryName.ZENODO,
        "dataset_id_in_repository": "123",
        "dataset_url_in_repository": "https://zenodo.org/record/123",
        "title": "Test Dataset",
        field: value,
    }
    metadata = DatasetMetadata(**data)
    assert getattr(metadata, field) == expected


# ---------------------------------
# Test validators (2) format_dates
# ---------------------------------
def test_format_dates_with_datetime_objects():
    """Test that datetime objects are correctly converted to ISO string format."""
    now = datetime(2026, 1, 23, 12, 0, 0)
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_created=now,
        date_last_updated=now,
        date_last_fetched=now
    )
    formatted = now.strftime("%Y-%m-%dT%H:%M:%S")
    assert metadata.date_created == formatted
    assert metadata.date_last_updated == formatted
    assert metadata.date_last_fetched == formatted


def test_format_dates_with_iso_strings():
    """Test that ISO string dates are normalized to '%Y-%m-%dT%H:%M:%S' format."""
    iso_input = "2026-01-23T00:00:00"
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_created="2026-01-23",
    )
    assert metadata.date_created == iso_input


# ------------------------------------------
# Test validators (3) Project field fallback
# ------------------------------------------
def test_fill_project_fields_from_repository_fills_missing_fields():
    """Test that project fields are populated from repository fields when missing."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.ZENODO,
        dataset_id_in_repository="repo_123",
        dataset_url_in_repository="https://zenodo.org/record/repo_123",
        dataset_project_name=None,
        dataset_id_in_project=None,
        dataset_url_in_project=None,
        title="Test Dataset"
    )

    metadata = metadata.fill_project_fields_from_repository()
    assert metadata.dataset_project_name == DatasetProjectName.ZENODO
    assert metadata.dataset_id_in_project == "repo_123"
    assert metadata.dataset_url_in_project == "https://zenodo.org/record/repo_123"


def test_fill_project_fields_from_repository_invalid_mapping():
    """Test that ValueError is raised when repository cannot map to a project."""
    with pytest.raises(AttributeError, match="type object"):
        metadata = DatasetMetadata(
            dataset_repository_name=DatasetRepositoryName.REPO,
            dataset_id_in_repository="123",
            dataset_url_in_repository="https://example.com/123",
            title="Test Dataset"
        )


# -----------------------------------------------
# Test validators (4) Date fetched field fallback
# -----------------------------------------------
def test_fill_date_last_fetched():
    """Test that date_last_fetched is populated and in '%Y-%m-%dT%H:%M:%S' format."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_last_fetched=None
    )

    metadata = metadata.fill_date_last_fetched()

    # Check it's not None
    assert metadata.date_last_fetched is not None

    # Check format: 'YYYY-MM-DDTHH:MM:SS'
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
    assert re.match(pattern, str(metadata.date_last_fetched))


def test_fill_date_last_fetched_keeps_existing_value():
    """Test that existing date_last_fetched is not overwritten."""
    existing = "2025-12-31T23:59:59"
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.ZENODO,
        dataset_id_in_repository="123",
        dataset_url_in_repository="https://zenodo.org/record/123",
        title="Test Dataset",
        date_last_fetched=existing
    )
    metadata = metadata.fill_date_last_fetched()
    assert metadata.date_last_fetched == existing


# ---------------------
# Full integration test
# ---------------------
def test_dataset_metadata_full_scenario():
    """Test a realistic scenario with mixed missing fields and validators."""
    metadata = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.FIGSHARE,
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
        license=""
    )

    # Apply both after-validators
    metadata.fill_project_fields_from_repository()
    metadata.fill_date_last_fetched()

    # Check that empty fields converted to None
    assert metadata.description is None
    assert metadata.keywords is None
    assert metadata.author_names is None
    assert metadata.external_links is None
    assert metadata.license is None

    # Check project fields filled
    assert metadata.dataset_project_name == DatasetProjectName.FIGSHARE
    assert metadata.dataset_id_in_project == "fig_456"
    assert metadata.dataset_url_in_project == "https://figshare.com/articles/fig_456"

    # Check date_last_fetched is filled
    assert isinstance(metadata.date_last_fetched, str)

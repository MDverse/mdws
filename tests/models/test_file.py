"""Tests for the pydantic file model module."""

import pytest
from pydantic import ByteSize, ValidationError

from mdverse_scrapers.models.enums import DatasetRepositoryName
from mdverse_scrapers.models.file import FileMetadata


def test_file_metadata_basic_creation():
    """Test creating a FileMetadata instance with minimal required fields."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        date_last_fetched="2026-01-21T12:00:00",
    )

    assert file.file_name == "file.xtc"
    assert file.file_type == "xtc"
    assert file.file_md5 is None
    assert file.file_size_in_bytes is None
    assert file.file_size_with_human_readable_unit is None


@pytest.mark.parametrize(
    ("raw_size", "expected_bytes", "expected_human"),
    [
        ("689 Bytes", 689, "689 B"),
        ("689Bytes", 689, "689 B"),
        ("3 MB", 3_000_000, "3.0 MB"),
        ("24.4 kB", 24_400, "24.4 KB"),
        (1024, 1024, "1.0 KB")
    ],
)
def test_file_size_normalization(
    raw_size: str,
    expected_bytes: int,
    expected_human: str,
) -> None:
    """Test file size normalization from various human-readable inputs."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        file_size_in_bytes=raw_size,
        date_last_fetched="2026-01-21T12:00:00",
    )

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == expected_bytes
    assert file.file_size_with_human_readable_unit == expected_human


def test_date_last_fetched_datetime_input():
    """Test that date_last_fetched returns correct format ISO string."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        date_last_fetched="2026-01-21",
    )
    assert file.date_last_fetched == "2026-01-21T00:00:00"


@pytest.mark.parametrize(
    ("file_name", "expected_file_type"),
    [
        ("file.xtc", "xtc"),
        ("archive.tar.gz", "tar.gz"),
        ("document.txt", "txt"),
    ],
)
def test_file_type_computed_correctly(file_name: str, expected_file_type: str) -> None:
    """Test that file_type is computed correctly from the file_name."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file",
        file_name=file_name,
        date_last_fetched="2026-01-21",
    )

    assert file.file_type == expected_file_type


def test_file_name_without_extension_raises_value_error() -> None:
    """Test that file_name without extension raises a ValueError."""
    with pytest.raises(ValidationError, match="String should match pattern"):
        FileMetadata(
            dataset_repository_name=DatasetRepositoryName.NOMAD,
            dataset_id_in_repository="abc123",
            dataset_url_in_repository="https://example.com",
            file_url_in_repository="https://example.com/file",
            file_name="file",
            date_last_fetched="2026-01-21",
        )

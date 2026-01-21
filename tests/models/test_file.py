"""Tests for the pydantic file model module."""
from datetime import datetime

from pydantic import ByteSize

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


def test_file_size_689_bytes():
    """Test file size normalization from '689 Bytes'."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        file_size_in_bytes='689 Bytes',
        date_last_fetched="2026-01-21T12:00:00",
    )    

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == 689
    assert file.file_size_with_human_readable_unit == '689 B'


def test_file_size_3mb():
    """Test file size normalization from '3 MB'."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        file_size_in_bytes="3 MB",
        date_last_fetched="2026-01-21T12:00:00",
    )

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == 3_000_000
    assert file.file_size_with_human_readable_unit == "3.0 MB"


def test_file_size_24_4_kb():
    """Test file size normalization from '24.4 kB'."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        file_size_in_bytes="24.4 kB",
        date_last_fetched="2026-01-21T12:00:00",
    )

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == 24_400
    assert file.file_size_with_human_readable_unit == "24.4 KB"


def test_file_size_integer_input():
    """Test file size normalization from integer input (bytes)."""
    file = FileMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_type="xtc",
        file_size_in_bytes=1024,
        date_last_fetched="2026-01-21T12:00:00",
    )

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == 1024
    assert file.file_size_with_human_readable_unit == "1.0 KB"


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

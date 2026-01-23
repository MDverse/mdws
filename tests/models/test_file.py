"""Tests for the pydantic file model module."""

import pytest
from pydantic import ByteSize

from mdverse_scrapers.models.enums import DatasetSourceName
from mdverse_scrapers.models.file import FileMetadata


def test_file_metadata_basic_creation():
    """Test creating a FileMetadata instance with minimal required fields."""
    file = FileMetadata(
        dataset_repository_name=DatasetSourceName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
    )
    assert file.file_name == "file.xtc"
    assert file.file_type == "xtc"
    assert file.file_md5 is None
    assert file.file_size_in_bytes is None
    assert file.file_size_with_human_readable_unit is None


@pytest.mark.parametrize(
    ("raw_size", "expected_size_in_bytes", "expected_size_with_human_readable_unit"),
    [
        ("689 Bytes", 689, "689 B"),
        ("123 bytes", 123, "123 B"),
        ("689Bytes", 689, "689 B"),
        ("3 MB", 3_000_000, "3.0 MB"),
        ("24.4 kB", 24_400, "24.4 KB"),
        (1024, 1024, "1.0 KB"),
    ],
)
def test_file_size_normalization(
    raw_size: str,
    expected_size_in_bytes: int,
    expected_size_with_human_readable_unit: str,
) -> None:
    """Test file size normalization."""
    file = FileMetadata(
        dataset_repository_name=DatasetSourceName.ZENODO,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file.xtc",
        file_name="file.xtc",
        file_size_in_bytes=raw_size,
    )

    assert isinstance(file.file_size_in_bytes, ByteSize)
    assert file.file_size_in_bytes == expected_size_in_bytes
    assert (
        file.file_size_with_human_readable_unit
        == expected_size_with_human_readable_unit
    )


@pytest.mark.parametrize(
    ("file_name", "expected_file_type"),
    [
        ("file.xtc", "xtc"),
        ("archive.tar.gz", "gz"),
        ("document.txt", "txt"),
        ("no_extension_file", ""),
        ("complex.name.with.many.dots.pdb", "pdb"),
        ("complex/path/to/file.pdb", "pdb"),
        ("very.complex/path/to/file.pdb", "pdb"),
        ("long path/with/some spaces/to/this_file.txt", "txt"),
    ],
)
def test_file_type_computed_correctly(file_name: str, expected_file_type: str) -> None:
    """Test that file_type is computed correctly from the file_name."""
    file = FileMetadata(
        dataset_repository_name=DatasetSourceName.NOMAD,
        dataset_id_in_repository="abc123",
        dataset_url_in_repository="https://example.com",
        file_url_in_repository="https://example.com/file",
        file_name=file_name,
    )
    assert file.file_type == expected_file_type

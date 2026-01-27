"""Tests for the toolbox module."""

from mdverse_scrapers.core.toolbox import (
    convert_file_size_in_bytes_to_human_readable_format,
)


class TestConvertFileSizeInBytesToHumanReadableFormat:
    """Tests for convert_file_size_in_bytes_to_human_readable_format function."""

    def test_bytes(self):
        """Test conversion for values in bytes range."""
        assert convert_file_size_in_bytes_to_human_readable_format(0) == "0.00 B"
        assert convert_file_size_in_bytes_to_human_readable_format(1) == "1.00 B"
        assert convert_file_size_in_bytes_to_human_readable_format(512) == "512.00 B"
        assert convert_file_size_in_bytes_to_human_readable_format(1023) == "1023.00 B"

    def test_kilobytes(self):
        """Test conversion for values in kilobytes range."""
        assert convert_file_size_in_bytes_to_human_readable_format(1024) == "1.00 KB"
        assert convert_file_size_in_bytes_to_human_readable_format(1536) == "1.50 KB"
        assert convert_file_size_in_bytes_to_human_readable_format(10240) == "10.00 KB"
        assert (
            convert_file_size_in_bytes_to_human_readable_format(127560) == "124.57 KB"
        )

    def test_megabytes(self):
        """Test conversion for values in megabytes range."""
        assert convert_file_size_in_bytes_to_human_readable_format(1048576) == "1.00 MB"
        assert convert_file_size_in_bytes_to_human_readable_format(1289748) == "1.23 MB"
        assert (
            convert_file_size_in_bytes_to_human_readable_format(10485760) == "10.00 MB"
        )
        assert (
            convert_file_size_in_bytes_to_human_readable_format(104857600)
            == "100.00 MB"
        )

    def test_gigabytes(self):
        """Test conversion for values in gigabytes range."""
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1073741824) == "1.00 GB"
        )
        assert (
            convert_file_size_in_bytes_to_human_readable_format(2147483648) == "2.00 GB"
        )
        assert (
            convert_file_size_in_bytes_to_human_readable_format(132553428173)
            == "123.45 GB"
        )

    def test_terabytes(self):
        """Test conversion for values in terabytes range."""
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1099511627776)
            == "1.00 TB"
        )  # 1 TB
        assert (
            convert_file_size_in_bytes_to_human_readable_format(5497558138880)
            == "5.00 TB"
        )  # 5 TB

    def test_very_large_file(self):
        """Test conversion for files larger than terabytes."""
        # 1 PB (petabyte)
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1125899906842624)
            == "File too big!"
        )

    def test_edge_cases(self):
        """Test edge cases between unit boundaries."""
        # Just at the boundary
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1024**2) == "1.00 MB"
        )  # Exactly 1 MB
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1024**3) == "1.00 GB"
        )  # Exactly 1 GB
        assert (
            convert_file_size_in_bytes_to_human_readable_format(1024**4) == "1.00 TB"
        )  # Exactly 1 TB

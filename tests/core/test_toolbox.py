"""Tests for the toolbox module."""

from mdverse_scrapers.core.toolbox import (
    convert_file_size_to_human_readable,
)


class TestConvertFileSizeInBytesToHumanReadableFormat:
    """Tests for convert_file_size_in_bytes_to_human_readable_format function."""

    def test_bytes(self):
        """Test conversion for values in bytes range."""
        assert convert_file_size_to_human_readable(0) == "0.00 B"
        assert convert_file_size_to_human_readable(1) == "1.00 B"
        assert convert_file_size_to_human_readable(512) == "512.00 B"
        assert convert_file_size_to_human_readable(789) == "789.00 B"

    def test_kilobytes(self):
        """Test conversion for values in kilobytes range."""
        assert convert_file_size_to_human_readable(1_024) == "1.02 KB"
        assert convert_file_size_to_human_readable(1_234) == "1.23 KB"
        assert convert_file_size_to_human_readable(123_456) == "123.46 KB"

    def test_megabytes(self):
        """Test conversion for values in megabytes range."""
        assert convert_file_size_to_human_readable(1_048_576) == "1.05 MB"
        assert convert_file_size_to_human_readable(10_485_760) == "10.49 MB"
        assert convert_file_size_to_human_readable(104_857_600) == "104.86 MB"

    def test_gigabytes(self):
        """Test conversion for values in gigabytes range."""
        assert convert_file_size_to_human_readable(1_000_000_000) == "1.00 GB"
        assert convert_file_size_to_human_readable(45_689_000_000) == "45.69 GB"
        assert convert_file_size_to_human_readable(132_553_428_173) == "132.55 GB"

    def test_terabytes(self):
        """Test conversion for values in terabytes range."""
        assert convert_file_size_to_human_readable(1_099_511_627_776) == "1.10 TB"
        assert convert_file_size_to_human_readable(5_497_558_138_880) == "5.50 TB"

    def test_very_large_file(self):
        """Test conversion for files larger than terabytes."""
        # 1 PB (petabyte)
        assert (
            convert_file_size_to_human_readable(1_000_000_000_000_000)
            == "File too big!"
        )

    def test_negative_size(self):
        """Test conversion for negative file size."""
        assert convert_file_size_to_human_readable(-500) == "Negative size!"

    def test_edge_cases(self):
        """Test edge cases between unit boundaries."""
        # Just at the boundary
        assert convert_file_size_to_human_readable(1000**2) == "1.00 MB"  # Exactly 1 MB
        assert convert_file_size_to_human_readable(1000**3) == "1.00 GB"  # Exactly 1 GB
        assert convert_file_size_to_human_readable(1000**4) == "1.00 TB"  # Exactly 1 TB

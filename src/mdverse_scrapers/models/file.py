"""Pydantic data models used to validate scraped molecular dynamics files."""

from pathlib import Path

from pydantic import ByteSize, ConfigDict, Field, computed_field, field_validator

from .dataset import DatasetCoreMetadata


# =====================================================================
# File-level metadata
# =====================================================================
class FileMetadata(DatasetCoreMetadata):
    """
    Pydantic model describing a single file belonging to a dataset.

    This model inherits core provenance information from DatasetCoreMetadata
    and defines file-specific metadata such as file name, extension...
    """

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    file_name: str = Field(
        ...,
        description="File name.",
    )
    file_url_in_repository: str = Field(
        ...,
        description="URL to access the file in the repository.",
    )
    file_size_in_bytes: ByteSize | None = Field(None, description="File size in bytes.")
    file_md5: str | None = Field(None, description="MD5 checksum.")
    containing_archive_file_name: str | None = Field(
        None,
        description=(
            "Name of the archive file the current file "
            "was extracted from, if applicable."
        ),
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("file_size_in_bytes", mode="before")
    @classmethod
    def normalize_byte_string(cls, value: int | str | None) -> int | str | None:
        """
        Normalize the unit "Bytes" with "B" to make it acceptable for ByteSize.

        Documentation: https://docs.pydantic.dev/latest/api/types/#pydantic.types.ByteSize

        Returns
        -------
        int | str | None
            The normalized "Bytes" file size as "B", or None if input is None.
        """
        if value is None:
            return None
        if isinstance(value, str) and "bytes" in value.lower():
            value = value.lower().replace("bytes", "B").strip()
        return value

    @computed_field
    @property
    def file_type(self) -> str:
        """Compute the file type from the file name.

        Returns
        -------
            str : The file extension computed from the file name.
        """
        extension = Path(self.file_name).suffix
        if extension.startswith("."):
            return extension[1:]
        else:
            return extension

    @computed_field
    @property
    def file_size_with_human_readable_unit(self) -> str | None:
        """
        Convert the file size in bytes into a human-readable format.

        Returns
        -------
            str | None : The size formatted with an appropriate unit
            (B, KB, MB, GB, or TB), rounded to two decimals.
        """
        if self.file_size_in_bytes is None:
            return None

        size = self.file_size_in_bytes
        return size.human_readable(decimal=True, separator=" ")

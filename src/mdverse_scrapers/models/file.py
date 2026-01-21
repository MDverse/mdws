"""Pydantic data models used to validate scraped molecular dynamics files.

This module defines strongly-typed Pydantic schemas that serve as a unified
data contract for MD files from  datasets collected from heterogeneous sources such as
Zenodo, Figshare, OSF, NOMAD, ATLAS, GPCRmd, and other domain-specific archives.

The models are designed to:
- Normalize metadata coming from different APIs and HTML structures
- Enforce consistent typing and field presence across sources
- Validate critical fields such as dates, URLs, and identifiers
- Provide a common representation that downstream tools can rely on
  regardless of the original data provider

These schemas are intended to be used as the final validation layer of
automated scraping pipelines, ensuring that extracted data is complete,
consistent, and ready for storage, indexing, or further analysis.
"""

from datetime import datetime

from pydantic import ByteSize, Field, computed_field, field_validator

from .dataset import DatasetCoreMetadata


# =====================================================================
# File-level metadata
# =====================================================================
class FileMetadata(DatasetCoreMetadata):
    """
    Pydantic model describing a single file belonging to a dataset.

    This model inherits core provenance information from DatasetCoreMetadata
    and defines file-specific metadata such as file identity, format, size,
    and semantic role within the dataset.
    """

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    file_url_in_repository: str = Field(
        ...,
        description="Canonical URL to access the file in the repository.",
    )
    file_name: str = Field(..., description="Name of the file in the dataset.")
    file_type: str = Field(
        ..., description="File extension (automatically deduced from name)."
    )
    file_size_in_bytes: ByteSize | None = Field(
        None, description="File size in bytes.")
    file_md5: str | None = Field(None, description="MD5 checksum.")
    date_last_fetched: str = Field(
        ..., description="Date when the file was last fetched."
    )
    containing_archive_file_name: str | None = Field(
        None,
        description="Archive file name this file was extracted from, if applicable.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("date_last_fetched", mode="before")
    def format_dates(cls, v: datetime | str) -> str:  # noqa: N805
        """Convert datetime objects or ISO strings to '%Y-%m-%dT%H:%M:%S' format.

        Parameters
        ----------
        cls : type[BaseDataset]
            The Pydantic model class being validated.
        v : str
            The input value of the 'date' field to validate.

        Returns
        -------
        str:
            The date in '%Y-%m-%dT%H:%M:%S' format.
        """
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%dT%H:%M:%S")
        return datetime.fromisoformat(v).strftime("%Y-%m-%dT%H:%M:%S")

    # @field_validator("file_size_in_bytes", mode="before")
    # def normalize_byte_string(cls, v: ByteSize | None) -> ByteSize | None:  # noqa: N805
    #     """
    #     Convert any input into a ByteSize object.

    #     - If it's a string containing "Bytes", replace with "B".
    #     - Let ByteSize parse strings like '24.4 kB', '3MB', '689 B'.
    #     - Integers are accepted as bytes directly.

    #     Returns
    #     -------
    #     ByteSize | None
    #         The normalized file size as a ByteSize object, or None if input is None.
    #     """
    #     if v is None:
    #         return None

    #     if isinstance(v, str) and "Bytes" in v:
    #         v = v.replace("Bytes", "b").strip()

    #     return ByteSize(v)

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
        # Assure ByteSize type
        size = self.file_size_in_bytes
        return size.human_readable(decimal=True, separator=" ")

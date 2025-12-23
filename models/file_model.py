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

from pydantic import BaseModel, Field, computed_field, field_validator

from scripts.toolbox import (
    DatasetRepository,
    format_date,
    validate_http_url,
)


# =====================================================================
# Base file class
# =====================================================================
class BaseFile(BaseModel):
    """
    Base Pydantic model for scraped molecular dynamics files.

    This class defines the common metadata schema shared by all supported
    repositories (e.g. Zenodo, Figshare, OSF, NOMAD, ATLAS, GPCRmd).

    Source-specific file models must inherit from this class and may
    extend it with additional fields or stricter validation rules.
    """

    # ------------------------------------------------------------------
    # Core provenance
    # ------------------------------------------------------------------
    dataset_repository: DatasetRepository = Field(
        ...,
        description=(
            "Name of the source repository. "
            "Allowed values: ZENODO, FIGSHARE, OSF, NOMAD, ATLAS, GPCRMD."
        ),
    )
    dataset_id_in_repository: str = Field(
        ...,
        description="Unique identifier of the dataset in the source repository.",
    )
    file_url_in_repository: str = Field(
        ...,
        description="Direct URL to access the file.",
    )

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    file_name: str = Field(
        ..., description="Name of the file in the dataset."
    )
    file_type: str = Field(
        ..., description="File extension (automatically deduced from name)."
    )
    file_size: int | None = Field(
        None, description="File size in bytes."
    )
    file_md5: str | None = Field(
        None, description="MD5 checksum."
    )
    date_last_fetched: str = Field(
        ..., description="Date when the file was last fetched."
    )
    extracted_from_archive: str | None = Field(
        None,
        description="Archive file name this file was extracted from, if applicable."
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
        return format_date(v)

    # To uncomment if u won't take time to valid all the file urls
    # @field_validator("file_url", mode="before")
    def valid_url(cls, v: str) -> str:  # noqa: N805
        """
        Validate that the URL field is a properly formatted HTTP/HTTPS URL.

        Parameters
        ----------
        cls : type[NomadFiles]
            The Pydantic model class being validated.
        v : str
            The input value of the 'url' field to validate.

        Returns
        -------
        str
            The validated URL string.
        """
        return validate_http_url(v)

    @computed_field
    @property
    def size_readable(self) -> str | None:
        """
        Convert the file size in bytes into a human-readable format.

        Returns
        -------
            str | None : The size formatted with an appropriate unit
            (B, KB, MB, GB, or TB), rounded to two decimals.
        """
        size = self.file_size
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        if size:
            while size >= 1024 and idx < len(units) - 1:
                size /= 1024
                idx += 1
            return f"{size:.2f} {units[idx]}"

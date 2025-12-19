"""Pydantic data models used to validate scraped molecular dynamics datasets.

This module defines strongly-typed Pydantic schemas that serve as a unified
data contract for MD datasets collected from heterogeneous sources such as
Zenodo, Figshare, OSF, NOMAD, ATLAS, GPCRmd, and other domain-specific archives.

The models are designed to:
- Normalize metadata coming from different APIs and HTML structures
- Enforce consistent typing and field presence across sources
- Validate critical fields such as dates, URLs, and identifiers
- Provide a common representation that downstream tools can rely on
  regardless of the original data provider

Each dataset-specific model (e.g. NomadDataset) captures both:
- Generic metadata shared across repositories (title, authors, dates, license)
- Source-specific attributes (e.g. force-field)

These schemas are intended to be used as the final validation layer of
automated scraping pipelines, ensuring that extracted data is complete,
consistent, and ready for storage, indexing, or further analysis.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

from scripts.toolbox import DatasetOrigin, format_date, validate_http_url

DOI = Annotated[
    str,
    StringConstraints(
        pattern=r"^10\.\d{4,9}/[\w\-.]+$"
    ),
]


# =====================================================================
# Base dataset class
# =====================================================================
class BaseDataset(BaseModel):
    """
    Base Pydantic model for scraped molecular dynamics datasets.

    This class defines the common metadata schema shared by all supported
    repositories (e.g. Zenodo, Figshare, OSF, NOMAD, ATLAS, GPCRmd).

    Source-specific dataset models must inherit from this class and may
    extend it with additional fields or stricter validation rules.
    """

    # ------------------------------------------------------------------
    # Core provenance
    # ------------------------------------------------------------------
    dataset_origin: DatasetOrigin = Field(
        ...,
        description=(
            "Name of the source repository. "
            "Allowed values: ZENODO, FIGSHARE, OSF, NOMAD, ATLAS, GPCRMD."
        ),
    )
    dataset_id: str = Field(
        ...,
        description="Unique identifier of the dataset in the source repository.",
    )
    dataset_url: str = Field(
        ...,
        description="Canonical URL to access the dataset.",
    )
    download_number: int | None = Field(
        None,
        description="Total number of downloads for the dataset.",
    )
    view_number: int | None = Field(
        None,
        description="Total number of views for the dataset.",
    )

    # ------------------------------------------------------------------
    # Temporal metadata
    # ------------------------------------------------------------------
    date_created: str | None = Field(
        None,
        description="Date when the dataset was originally created.",
    )
    date_last_updated: str | None = Field(
        None,
        description="Date when the dataset metadata was last updated.",
    )
    date_last_crawled: str = Field(
        ...,
        description="Date when the dataset was last crawled by the pipeline.",
    )

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    title: str = Field(
        ...,
        description="Title of the dataset.",
    )
    author_names: list[str] = Field(
        ...,
        description="List of author or contributor names.",
    )
    description: str | None = Field(
        None,
        description="Abstract or description of the dataset.",
    )
    keywords: list[str] | None = Field(
        None,
        description="List of keywords describing the dataset."
    )
    license: str | None = Field(
        None,
        description="License under which the dataset is distributed.",
    )
    doi: DOI | None = Field(
        default=None,
        description=(
            "Digital Object Identifier (DOI) of the dataset. "
            "Must start with '10.' and follow the standard DOI format."
        ),
    )
    links: list[str] | None = Field(
        None,
        description="External links to papers or other databases.",
    )

    # ------------------------------------------------------------------
    # File-level metadata
    # ------------------------------------------------------------------
    nb_files: int = Field(
        ...,
        description="Total number of files in the dataset.",
    )
    file_names: list[str] = Field(
        ...,
        description="List of dataset file names.",
    )

    # ------------------------------------------------------------------
    # Simulation metadata
    # ------------------------------------------------------------------
    simulation_program: str | None = Field(
        None,
        description="Molecular dynamics engine used (e.g. GROMACS, NAMD).",
    )
    simulation_program_version: str | None = Field(
        None,
        description="Version of the simulation engine.",
    )
    nb_atoms: int | None = Field(
        None,
        description="Total number of atoms in the simulated system.",
    )
    molecules: list[str] | None = Field(
        None,
        description="Molecular composition of the system, if available.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator(
        "date_created", "date_last_updated", "date_last_crawled", mode="before"
    )
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

    # To uncomment if u won't take time to valid all the dataset urls
    # @field_validator("url", mode="before")
    def validate_url(cls, v: str) -> str:  # noqa: N805
        """
        Validate that the URL field is a properly formatted HTTP/HTTPS URL.

        Parameters
        ----------
        cls : type[BaseDataset]
            The Pydantic model class being validated.
        v : str
            The input value of the 'url' field to validate.

        Returns
        -------
        str
            The validated URL string.
        """
        return validate_http_url(v)

    @field_validator(
            "description", "keywords", "links", "license", "molecules", mode="before")
    def empty_to_none(cls, v: list | str) -> list | str | None:  # noqa: N805
        """
        Normalize empty field values by converting them to None.

        Parameters
        ----------
        cls : type[BaseDataset]
            The Pydantic model class being validated.
        v : Optional[list | str]
            The raw input value of the field before conversion.
            Can be a list, a string, or None.

        Returns
        -------
        list | str | None
            Returns None if the value is an empty list or empty string;
            otherwise returns the original value.
        """
        if v == [] or v == "":
            return None
        return v

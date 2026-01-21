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

These schemas are intended to be used as the final validation layer of
automated scraping pipelines, ensuring that extracted data is complete,
consistent, and ready for storage, indexing, or further analysis.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

from .enums import DatasetProjectName, DatasetRepositoryName

DOI = Annotated[
    str,
    StringConstraints(pattern=r"^10\.\d{4,9}/[\w\-.]+$"),
]


# =====================================================================
# Core provenance metadata
# =====================================================================
class DatasetCoreMetadata(BaseModel):
    """
    Core provenance metadata shared by dataset-level and file-level models.

    This model defines the minimal set of identifiers and URLs required
    to uniquely reference a dataset within its source repository and,
    optionally, within a higher-level project.
    """

    dataset_repository_name: DatasetRepositoryName = Field(
        ...,
        description=(
            "Name of the source data repository. "
            "Allowed values in DatasetRepositoryName enum. "
            "Examples: ZENODO, FIGSHARE, OSF, NOMAD, MDPOSIT..."
        ),
    )
    dataset_id_in_repository: str = Field(
        ...,
        description="Unique identifier of the dataset in the source repository.",
    )
    dataset_url_in_repository: str = Field(
        ...,
        description="Canonical URL to access the dataset in the repository.",
    )
    dataset_project_name: DatasetProjectName | None = Field(
        None, exclude_if=lambda v: v is None,
        description=(
            "Name of the source data project. "
            "Allowed values in DatasetProjectName enum. "
            "Examples: ZENODO, FIGSHARE, OSF, NOMAD, MDDB..."
        ),
    )
    dataset_id_in_project: str | None = Field(
        None, exclude_if=lambda v: v is None,
        description="Unique identifier of the dataset in the project.",
    )
    dataset_url_in_project: str | None = Field(
        None, exclude_if=lambda v: v is None,
        description="Canonical URL to access the dataset in the project.",
    )


# =====================================================================
# Dataset-level metadata
# =====================================================================
class DatasetMetadata(DatasetCoreMetadata):
    """
    Base Pydantic model for scraped molecular dynamics datasets.

    This model extends DatasetCoreMetadata with dataset-specific metadata
    such as descriptive information, temporal fields, statistics, and
    simulation-related parameters.

    Repository-specific dataset models must inherit from this class and
    may extend it with additional fields or stricter validation rules.
    """

    # ------------------------------------------------------------------
    # Statistics metadata
    # ------------------------------------------------------------------
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
    date_last_fetched: str = Field(
        ...,
        description="Date when the dataset was last fetched by the pipeline.",
    )

    # ------------------------------------------------------------------
    # Descriptive metadata
    # ------------------------------------------------------------------
    title: str = Field(
        ...,
        description="Title of the dataset.",
    )
    author_names: list[str] | None = Field(
        None,
        description="List of author or contributor names.",
    )
    description: str | None = Field(
        None,
        description="Abstract or description of the dataset.",
    )
    keywords: list[str] | None = Field(
        None, description="List of keywords describing the dataset."
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
    external_links: list[str] | None = Field(
        None,
        description="External links to papers or other databases.",
    )

    # ------------------------------------------------------------------
    # File-level metadata
    # ------------------------------------------------------------------
    nb_files: int | None = Field(
        None,
        description="Total number of files in the dataset.",
    )

    # ------------------------------------------------------------------
    # Simulation metadata
    # ------------------------------------------------------------------
    software_name: str | None = Field(
        None,
        description="Molecular dynamics engine used (e.g. GROMACS, NAMD).",
    )
    software_version: str | None = Field(
        None,
        description="Version of the simulation engine.",
    )
    nb_atoms: int | None = Field(
        None,
        description="Total number of atoms in the simulated system.",
    )
    molecule_names: list[str] | None = Field(
        None,
        description="Molecular composition of the system, if available.",
    )
    forcefield_model_name: str | None = Field(
        None,
        description="Molecular dynamics forcefield model used (e.g. AMBER).",
    )
    forcefield_model_version: str | None = Field(
        None,
        description="Version of the forcefield model.",
    )
    simulation_timestep: float | None = Field(
        None, description="The time interval between new positions computation (in fs)."
    )
    simulation_time: list[str] | None = Field(
        None, description="The accumulated simulation time (in μs)."
    )
    simulation_temperature: list[str] | None = Field(
        None, description="The temperature chosen for the simulations (in K ou °C)."
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator(
        "date_created", "date_last_updated", "date_last_fetched", mode="before"
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
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%dT%H:%M:%S")
        return datetime.fromisoformat(v).strftime("%Y-%m-%dT%H:%M:%S")

    @field_validator(
        "description",
        "keywords",
        "external_links",
        "license",
        "author_names",
        "molecule_names",
        mode="before",
    )
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

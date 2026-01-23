"""Pydantic data models used to validate scraped molecular dynamics datasets."""

from datetime import datetime
from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    computed_field,
    field_validator,
    model_validator,
)

from .date import DATETIME_FORMAT
from .enums import DatasetSourceName
from .simulation import SimulationMetadata

DOI = Annotated[
    str,
    StringConstraints(pattern=r"^10\.\d{4,9}/[\w\-.]+$"),
]


# =====================================================================
# Core provenance metadata
# =====================================================================
class DatasetCoreMetadata(BaseModel):
    """
    Core provenance metadata shared by dataset and file models.

    This model captures essential information about the source repository
    """

    dataset_repository_name: DatasetSourceName = Field(
        ...,
        description=(
            "Name of the source data repository. "
            "Allowed values in the DatasetRepoProjectName enum. "
            "Examples: ZENODO, FIGSHARE, NOMAD..."
        ),
    )
    dataset_id_in_repository: str = Field(
        ...,
        description="Identifier of the dataset in the source repository.",
    )
    dataset_url_in_repository: str = Field(
        ...,
        description="URL to access the dataset in the repository.",
    )


# =====================================================================
# Dataset-level metadata
# =====================================================================
class DatasetMetadata(SimulationMetadata, DatasetCoreMetadata):
    """
    Base Pydantic model for molecular dynamics datasets.

    This model extends DatasetCoreMetadata with dataset-specific metadata.
    """

    # ------------------------------------------------------------------
    # Project metadata
    # ------------------------------------------------------------------
    dataset_project_name: DatasetSourceName | None = Field(
        None,
        description=(
            "Name of the source data project. "
            "Allowed values in the DatasetSourceName enum. "
            "Examples: ZENODO, FIGSHARE, NOMAD..."
        ),
    )
    dataset_id_in_project: str | None = Field(
        None,
        description="Identifier of the dataset in the project.",
    )
    dataset_url_in_project: str | None = Field(
        None,
        description="URL to access the dataset in the project.",
    )

    # ------------------------------------------------------------------
    # Statistics metadata
    # ------------------------------------------------------------------
    download_number: int | None = Field(
        None,
        gt=0,
        description="Total number of downloads for the dataset.",
    )
    view_number: int | None = Field(
        None,
        gt=0,
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

    @computed_field
    @property
    def date_last_fetched(self) -> str:
        """
        Populate the date when the dataset metadata was last fetched.

        Returns
        -------
        str
            The current date and time in ISO 8601 format.
        """
        return datetime.now().strftime(DATETIME_FORMAT)

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
        description="Description of the dataset.",
    )
    keywords: list[str] | None = Field(
        None, description="List of keywords describing the dataset."
    )
    license: str | None = Field(
        None,
        description="License under which the dataset is distributed.",
    )
    doi: DOI | None = Field(
        None,
        description=(
            "Digital Object Identifier (DOI) of the dataset. "
            "Must start with '10.' and follow the DOI format."
        ),
    )
    external_links: list[str] | None = Field(
        None,
        description="External links to publications or other databases.",
    )

    # ------------------------------------------------------------------
    # File-level metadata
    # ------------------------------------------------------------------
    number_of_files: int | None = Field(
        None,
        gt=0,
        description="Total number of files in the dataset.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("date_created", "date_last_updated", mode="before")
    @classmethod
    def format_dates(cls, value: datetime | str | None) -> str | None:
        """Convert datetime objects or strings to '%Y-%m-%dT%H:%M:%S' format.

        Parameters
        ----------
        cls : type[BaseDataset]
            The Pydantic model class being validated.
        value : datetime | str | None
            The input value of the 'date' field to validate.

        Returns
        -------
        str:
            The date in '%Y-%m-%dT%H:%M:%S' format.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        return datetime.fromisoformat(value).strftime(DATETIME_FORMAT)

    @field_validator(
        "description",
        "keywords",
        "external_links",
        "license",
        "author_names",
        mode="before",
    )
    @classmethod
    def empty_to_none(cls, value: list | str | None) -> list | str | None:
        """
        Normalize empty fields to None.

        Parameters
        ----------
        cls : type[BaseDataset]
            The Pydantic model class being validated.
        value : Optional[list | str]
            The raw input value of the field before conversion.
            Can be a list, a string, or None.

        Returns
        -------
        list | str | None
            Returns None if the value is an empty list or empty string;
            otherwise returns the original value.
        """
        if value == [] or value == "":
            return None
        return value

    @model_validator(mode="after")
    def fill_project_fields_from_repository(self) -> "DatasetMetadata":
        """
        Fallback project metadata to repository metadata when missing.

        Returns
        -------
        DatasetMetadata
            The validated model instance with project fields filled from
            repository fields when missing.
        """
        # Use repository name is project name is missing.
        if self.dataset_project_name is None:
            self.dataset_project_name = self.dataset_repository_name
        # Use repository identifier is project identifier is missing.
        if self.dataset_id_in_project is None:
            self.dataset_id_in_project = self.dataset_id_in_repository
        # Use repository URL is project URL is missing.
        if self.dataset_url_in_project is None:
            self.dataset_url_in_project = self.dataset_url_in_repository
        return self

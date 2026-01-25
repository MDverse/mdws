"""Pydantic data models for scrapers context."""

from datetime import datetime
from pathlib import Path
from typing import Self

from pydantic import BaseModel, Field, computed_field, model_validator

from .enums import DatasetSourceName, DataType


class ScraperContext(BaseModel):
    """Pydantic model describing the context of a scraper."""

    data_source_name: DatasetSourceName = Field(
        ...,
        description="Data repository to be scraped.",
    )
    output_dir_path: str | Path = Field(
        ...,
        description="Output directory path for the scraper results.",
    )
    log_file_path: str | Path | None = Field(
        None,
        description="Path to the log file for the scraper.",
    )
    datasets_parquet_file_path: str | Path | None = Field(
        None,
        description="Path to the output parquet file for datasets metadata.",
    )
    files_parquet_file_path: str | Path | None = Field(
        None,
        description="Path to the output parquet file for files metadata.",
    )
    token: str | None = Field(
        None,
        description="Access token or API key.",
    )

    @computed_field
    @property
    def start_time(self) -> datetime:
        """Datetime when the scraper context was created."""
        return datetime.now()

    @model_validator(mode="after")
    def create_output_dir_path(self) -> Self:
        """
        Create output directory path if it does not exist.

        Returns
        -------
        Self
            The ScraperContext instance with updated paths.
        """
        # Update and create output directory path.
        self.output_dir_path = (
            Path(self.output_dir_path)
            / self.data_source_name.value
            / datetime.now().strftime("%Y%m%d")
        )
        self.output_dir_path.mkdir(parents=True, exist_ok=True)
        # Define log file path.
        self.log_file_path = (
            self.output_dir_path / f"{self.data_source_name.value}_scraper.log"
        )
        # Define output parquet file path.
        self.datasets_parquet_file_path = (
            self.output_dir_path
            / f"{self.data_source_name.value}_{DataType.DATASETS.value}.parquet"
        )
        self.files_parquet_file_path = (
            self.output_dir_path
            / f"{self.data_source_name.value}_{DataType.FILES.value}.parquet"
        )
        return self

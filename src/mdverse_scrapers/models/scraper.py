"""Pydantic data models for scrapers context."""

from datetime import datetime
from pathlib import Path
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
    model_validator,
)

from .enums import DatasetSourceName, DataType


class ScraperContext(BaseModel):
    """Pydantic model describing the context of a scraper.

    Mandatory fields are `data_source_name` and `output_dir_path`.
    """

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    data_source_name: DatasetSourceName = Field(
        ...,
        description="Data repository to be scraped.",
    )
    output_dir_path: DirectoryPath = Field(
        ...,
        description="Output directory path for the scraper results.",
    )
    query_file_path: FilePath | None = Field(
        None,
        description="Path to the query file for the scraper.",
    )
    log_file_path: Path = Field(
        Path(""),
        description="Path to the log file for the scraper.",
    )
    datasets_parquet_file_path: Path = Field(
        Path(""),
        description="Path to the output parquet file for datasets metadata.",
    )
    number_of_datasets_scraped: int = Field(
        0,
        ge=0,
        description="Number of datasets scraped.",
    )
    files_parquet_file_path: Path = Field(
        Path(""),
        description="Path to the output parquet file for files metadata.",
    )
    number_of_files_scraped: int = Field(
        0,
        ge=0,
        description="Number of files scraped.",
    )
    token: str | None = Field(
        None,
        description="Access token or API key.",
    )
    start_time: datetime = Field(
        default_factory=lambda: datetime.now(),
        description="Datetime when the scraper started.",
    )

    @model_validator(mode="after")
    def define_output_dir_file_paths(self) -> Self:
        """
        Define output subdirectory path and output files paths.

        Returns
        -------
        Self
            The ScraperContext instance with updated paths.
        """
        # Update and create output subdirectory path.
        self.output_dir_path = (
            Path(self.output_dir_path)
            / self.data_source_name.value
            / self.start_time.strftime("%Y-%m-%d")
        )
        self.output_dir_path.mkdir(parents=True, exist_ok=True)
        # Define log file path.
        self.log_file_path = (
            self.output_dir_path / f"{self.data_source_name.value}_scraper.log"
        )
        # Define output parquet file path for datasets and files metadata.
        self.datasets_parquet_file_path = (
            self.output_dir_path
            / f"{self.data_source_name.value}_{DataType.DATASETS.value}.parquet"
        )
        self.files_parquet_file_path = (
            self.output_dir_path
            / f"{self.data_source_name.value}_{DataType.FILES.value}.parquet"
        )
        return self

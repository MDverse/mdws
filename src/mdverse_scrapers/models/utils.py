"""Utils for Pydantic models."""

from pathlib import Path
from typing import Any

import loguru
import pandas as pd
from pydantic import ValidationError

from .dataset import DatasetMetadata
from .file import FileMetadata


def validate_metadata_against_model(
    metadata: dict[str, Any],
    model: type[FileMetadata | DatasetMetadata],
    logger: "loguru.Logger" = loguru.logger,
) -> FileMetadata | DatasetMetadata | None:
    """Validate metadata against a Pydantic model.

    Parameters
    ----------
    metadata: dict[str, Any]
        Metadata to validate.
    model: type[FileMetadata | DatasetMetadata]
        Pydantic model used for the validation.

    Returns
    -------
    type[FileMetadata | DatasetMetadata] | None
        Validated model instance or None if validation fails.
    """
    try:
        return model.model_validate(metadata)
    except ValidationError as exc:
        logger.warning("Validation error!")
        for error in exc.errors():
            field = error["loc"]
            logger.debug(f"Field: {field[0]}")
            if len(field) > 1:
                logger.debug(f"Subfield: {field[1]}")
            logger.debug(f"Error type: {error.get('input')}")
            logger.debug(f"Reason: {error['msg']}")
            inputs = error["input"]
            if not isinstance(inputs, dict):
                logger.debug(f"Input value: {inputs}")
            else:
                logger.debug("Input is a complex structure. Skipping value display.")
        return None


def normalize_datasets_metadata(
    datasets: list[dict],
    logger: "loguru.Logger" = loguru.logger,
) -> list[DatasetMetadata]:
    """
    Normalize dataset metadata with a Pydantic model.

    Parameters
    ----------
    datasets : list[dict]
        List of dataset metadata dictionaries.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[DatasetMetadata]
        List of successfully validated `DatasetMetadata` objects.
    """
    datasets_metadata = []
    for dataset in datasets:
        logger.info(
            f"Normalizing metadata for dataset: {dataset['dataset_id_in_repository']}"
        )
        normalized_metadata = validate_metadata_against_model(
            dataset, DatasetMetadata, logger=logger
        )
        if not normalized_metadata:
            logger.error(
                f"Metadata normalization failed for dataset "
                f"{dataset['dataset_id_in_repository']}"
            )
            continue
        datasets_metadata.append(normalized_metadata)
    logger.info(
        "Normalized metadata for "
        f"{len(datasets_metadata)}/{len(datasets)} "
        f"({len(datasets_metadata) / len(datasets):.0%}) datasets."
    )
    return datasets_metadata


def normalize_files_metadata(
    files: list[dict],
    logger: "loguru.Logger" = loguru.logger,
) -> list[FileMetadata]:
    """
    Normalize file metadata with a Pydantic model.

    Parameters
    ----------
    files : list[dict]
        List of file metadata dictionaries.
    logger: "loguru.Logger"
        Logger object.

    Returns
    -------
    list[FileMetadata]
        List of successfully validated `FileMetadata` objects.
    """
    files_metadata = []
    for file_meta in files:
        logger.info(
            "Normalizing metadata for files in dataset: "
            f"{file_meta['dataset_id_in_repository']}"
        )
        normalized_metadata = validate_metadata_against_model(
            file_meta, FileMetadata, logger=logger
        )
        if not normalized_metadata:
            logger.error(
                f"Metadata normalization failed for file: {file_meta['file_name']}"
            )
            logger.info(f"In dataset: {file_meta['dataset_id_in_repository']}")
            continue
        files_metadata.append(normalized_metadata)
        logger.info(
            "Normalized metadata for "
            f"{len(files_metadata)}/{len(files)} "
            f"({len(files_metadata) / len(files):.0%}) files."
        )
    return files_metadata


def export_list_of_models_to_parquet(
    parquet_path: Path,
    list_of_models: list[DatasetMetadata] | list[FileMetadata],
    logger: "loguru.Logger" = loguru.logger,
) -> int:
    """Export list of Pydantic models to parquet file.

    Parameters
    ----------
    parquet_path : Path
        Path to the output parquet file.
    list_of_models : list[DatasetMetadata] | list[FileMetadata]
        List of Pydantic models to export.
    logger : "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    int
        Number of exported models.
    """
    logger.info("Exporting models to parquet.")
    try:
        df = pd.DataFrame([model.model_dump() for model in list_of_models])
        df.to_parquet(parquet_path, index=False)
        logger.success(f"Exported {len(df):,} rows to:")
        logger.success(parquet_path)
        return len(df)
    except (ValueError, TypeError, OSError) as e:
        logger.error("Failed to export models to parquet.")
        logger.error(e)
        return 0

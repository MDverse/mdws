"""Utils for Pydantic models."""

from typing import Any

import loguru
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
        The metadatas to validate.
    model: type[FileMetadata | DatasetMetadata]
        The Pydantic model used for the validation.

    Returns
    -------
    type[FileMetadata | DatasetMetadata] | None
        Validated model instance or None if validation fails.
    """
    try:
        return model(**metadata)
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

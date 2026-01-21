"""Tests for the pydantic dataset model module."""

import pytest

from mdverse_scrapers.models.dataset import DatasetCoreMetadata, DatasetMetadata
from mdverse_scrapers.models.enums import DatasetProjectName, DatasetRepositoryName


def test_dataset_core_metadata_minimal():
    """Test creation with only required fields."""
    dataset = DatasetCoreMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv"
    )

    assert dataset.dataset_repository_name == DatasetRepositoryName.NOMAD
    assert dataset.dataset_id_in_repository == "dNdV1k67vGSN1DUhrBeOSvJeBnvv"
    assert dataset.dataset_url_in_repository == "https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv"

    # Optional fields default to None
    assert dataset.dataset_project_name is None
    assert dataset.dataset_id_in_project is None
    assert dataset.dataset_url_in_project is None

    # model_dump with exclude_none should remove optional fields
    dumped = dataset.model_dump()
    assert "dataset_project_name" not in dumped
    assert "dataset_id_in_project" not in dumped
    assert "dataset_url_in_project" not in dumped


def test_dataset_core_metadata_full():
    """Test creation with all fields."""
    dataset = DatasetCoreMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="NBOCe85s8_8stAsSeav4Do7FGiPs",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=NBOCe85s8_8stAsSeav4Do7FGiPs",
        dataset_project_name=DatasetProjectName.NOMAD,
        dataset_id_in_project="proj001",
        dataset_url_in_project="https://example.com/project/proj001",
    )

    assert dataset.dataset_project_name == DatasetProjectName.NOMAD
    assert dataset.dataset_id_in_project == "proj001"
    assert dataset.dataset_url_in_project == "https://example.com/project/proj001"

    dumped = dataset.model_dump()
    # All fields should remain since none are None
    assert "dataset_project_name" in dumped
    assert "dataset_id_in_project" in dumped
    assert "dataset_url_in_project" in dumped


def test_dataset_metadata_with_additional_fields():
    """Test DatasetMetadata including extra fields like title, date_created, etc."""
    dataset = DatasetMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        title="VASP MolecularDynamics simulation",
        date_last_fetched="2025-12-23T17:24:33",
    )

    # Required DatasetMetadata fields
    assert dataset.title == "VASP MolecularDynamics simulation"
    assert dataset.date_last_fetched == "2025-12-23T17:24:33"

    # Optional simulation fields default to None
    assert dataset.software_name is None
    assert dataset.nb_atoms is None

    dumped = dataset.model_dump(exclude_none=True)
    # Fields with None should be removed
    assert "software_name" not in dumped
    assert "nb_atoms" not in dumped
    # Fields with values should remain
    assert "title" in dumped
    assert "dataset_repository_name" in dumped


def test_invalid_types():
    """Test that invalid types raise a validation error."""
    from pydantic import ValidationError

    # repository_name must be from the enum
    with pytest.raises(ValidationError):
        DatasetCoreMetadata(
            dataset_repository_name="invalid_repo",  # invalid type
            dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
            dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        )

    # Optional field accepts None
    dataset = DatasetCoreMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_project_name=None,
    )
    assert dataset.dataset_project_name is None

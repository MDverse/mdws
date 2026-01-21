"""Tests for the pydantic dataset model module."""
import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.dataset import (
    DatasetCoreMetadata,
    DatasetMetadata,
    SimulationMetadata,
)
from mdverse_scrapers.models.enums import DatasetProjectName, DatasetRepositoryName


# -------------------------------
# Tests for dataset core metadata
# -------------------------------
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


# --------------------------
# Tests for dataset metadata
# --------------------------
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
    assert dataset.number_of_atoms is None

    dumped = dataset.model_dump(exclude_none=True)
    # Fields with None should be removed
    assert "software_name" not in dumped
    assert "number_of_atoms" not in dumped
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


# ------------------------------------------------
# Tests for positive values in simulation metadata
# ------------------------------------------------
def test_validate_positive_simulation_values_float():
    """Test numeric values are accepted if positive."""
    obj = SimulationMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        simulation_timestep=0.5,
        simulation_time=[1.2]
    )
    assert obj.simulation_timestep == 0.5
    assert obj.simulation_time == [1.2]


def test_validate_positive_simulation_values_str_with_units():
    """Test string numeric values with units are accepted."""
    obj = SimulationMetadata(
        dataset_repository_name=DatasetRepositoryName.NOMAD,
        dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
        simulation_timestep="0.1fs",
        simulation_time=["0.5fs", "1.0fs"]
    )
    assert obj.simulation_timestep == "0.1fs"
    assert obj.simulation_time == ["0.5fs", "1.0fs"]


def test_validate_positive_simulation_values_negative():
    """Test negative numbers raise validation error."""
    with pytest.raises(ValidationError):
        SimulationMetadata(
            dataset_repository_name=DatasetRepositoryName.NOMAD,
            dataset_id_in_repository="dNdV1k67vGSN1DUhrBeOSvJeBnvv",
            dataset_url_in_repository="https://nomad-lab.eu/prod/v1/gui/search/entries?entry_id=dNdV1k67vGSN1DUhrBeOSvJeBnvv",
            simulation_timestep=-1.0
        )


# ---------------------------------------------------
# Tests for temperature values in simulation metadata
# ---------------------------------------------------
def test_normalize_temperatures_single_kelvin():
    """Test a single temperature given in Kelvin."""
    temp = "300K"
    normalized = SimulationMetadata.normalize_temperatures(temp)
    assert normalized == [300.0]


def test_normalize_temperatures_single_celsius():
    """Test a single temperature given in Celsius."""
    temp = "27°C"
    normalized = SimulationMetadata.normalize_temperatures(temp)
    assert normalized == [300.15]


def test_normalize_temperatures_no_unit_above_273():
    """Test a temperature with no unit assumed to be Kelvin if >= 273."""
    temp = "280"
    normalized = SimulationMetadata.normalize_temperatures(temp)
    assert normalized == [280.0]


def test_normalize_temperatures_no_unit_below_273():
    """Test a temperature with no unit assumed to be Celsius if < 273."""
    temp = "25"
    normalized = SimulationMetadata.normalize_temperatures(temp)
    assert normalized == [298.15]


def test_normalize_temperatures_list_mixed_units():
    """Test a list of temperatures with mixed units."""
    temps = ["25°C", "300K", "50"]
    normalized = SimulationMetadata.normalize_temperatures(temps)
    expected = [298.15, 300.0, 323.15]
    assert normalized == expected


def test_normalize_temperatures_none():
    """Test that None input returns None."""
    normalized = SimulationMetadata.normalize_temperatures(None)
    assert normalized is None

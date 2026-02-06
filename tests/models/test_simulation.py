"""Tests for the Pydantic simulation model."""

import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.enums import ExternalDatabaseName
from mdverse_scrapers.models.simulation import (
    ExternalIdentifier,
    ForceFieldModel,
    Molecule,
    SimulationMetadata,
    Software,
)


# --------------------------------------------------
# Test simulation timestep and time positive values
# --------------------------------------------------
@pytest.mark.parametrize(
    ("values", "should_raise_exception"),
    [
        ([0.1, 2.0], False),
        ([0, 1.0], True),  # because 0 <= 0
        ([-1, 2.0], True),  # because -1 <= 0
        (None, False),
    ],
)
def test_positive_simulation_values(values, should_raise_exception):
    """Test that simulation numeric parameters must be strictly positive."""
    if should_raise_exception:
        with pytest.raises((ValueError, ValidationError), match=r"Simulation|Invalid"):
            SimulationMetadata(simulation_timesteps_in_fs=values)
    else:
        metadata = SimulationMetadata(simulation_timesteps_in_fs=values)
        assert metadata.simulation_timesteps_in_fs == values


# ------------------------------
# Test temperature normalization
# ------------------------------
@pytest.mark.parametrize(
    ("test_temp", "expected_temp_in_kelvin"),
    [
        (["300K", "300  K"], [300.0, 300.0]),
        (["27°C", "27 °C"], [300.15, 300.15]),
        (["0c", "100 Celcius"], [273.15, 373.15]),
        (["-10C", "-10 °C", "-1.87"], [263.15, 263.15, 271.28]),
        (None, None),
    ],
)
def test_temperature_normalization(test_temp, expected_temp_in_kelvin):
    """Test normalization of simulation_temperature to Kelvin."""
    metadata = SimulationMetadata(simulation_temperatures_in_kelvin=test_temp)
    assert metadata.simulation_temperatures_in_kelvin == expected_temp_in_kelvin


# ----------------------------------------------
# Test software, molecules, forcefields creation
# ----------------------------------------------
def test_structured_fields_creation():
    """Test that software, molecules, and forcefields can be created."""
    metadata = SimulationMetadata(
        software=[Software(name="GROMACS", version="2023.1")],
        molecules=[
            Molecule(
                name="H2O",
                number_of_atoms=3,
                formula="H2O",
                number_of_molecules=100,
                sequence="PEPTIDE",
                external_identifiers=[
                    ExternalIdentifier(
                        database_name=ExternalDatabaseName.PDB, identifier="1ABC"
                    )
                ],
            )
        ],
        forcefields_models=[ForceFieldModel(name="AMBER", version="ff14SB")],
    )
    assert metadata.software[0].name == "GROMACS"
    assert metadata.molecules[0].number_of_atoms == 3
    assert metadata.molecules[0].number_of_molecules == 100
    assert metadata.forcefields_models[0].version == "ff14SB"
    assert metadata.molecules[0].sequence == "PEPTIDE"
    assert (
        metadata.molecules[0].external_identifiers[0].database_name
        == ExternalDatabaseName.PDB
    )
    assert metadata.molecules[0].external_identifiers[0].identifier == "1ABC"


# -------------------
# Test invalid fields
# -------------------
def test_invalid_fields():
    """Test with a non-existing fields."""
    with pytest.raises(ValidationError):
        SimulationMetadata(total_number_of_something=1000)


# --------------------------------------
# Test invalid simulation parameter type
# --------------------------------------
def test_invalid_simulation_value_type():
    """Test that non-numeric strings raise ValidationError."""
    with pytest.raises(ValidationError):
        SimulationMetadata(
            simulation_timesteps_in_fs=["invalid-value"],  # because not a float
        )

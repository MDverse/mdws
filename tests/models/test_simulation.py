"""Tests for the pydantic dataset model module."""

import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.simulation import SimulationMetadata


# -------------------------------------------------------------------
# Test simulation timestep and time positive values
# -------------------------------------------------------------------
@pytest.mark.parametrize(
    ("values", "should_raise"),
    [
        ([0.1, 2.0], False),
        ([0, 1.0], True),  # because <= 0
        ([-1, 2.0], True),
        (["-0.1"], True),
        (None, False),
    ],
)
def test_positive_simulation_values(values, should_raise):
    """Test that simulation numeric parameters must be strictly positive."""
    if should_raise:
        with pytest.raises((ValueError, ValidationError),
                            match=r"Simulation|Invalid"
        ):
            SimulationMetadata(simulation_timestep_in_fs=values)
    else:
        meta = SimulationMetadata(simulation_timestep_in_fs=values)
        assert meta.simulation_timestep_in_fs == values


# -------------------------------------------------------------------
# Test temperature normalization
# -------------------------------------------------------------------
@pytest.mark.parametrize(
    ("temps_input", "expected_kelvin"),
    [
        (["300K"], [300.0]),
        (["27°C"], [300.15]),
        (["0c", "100 Celcius"], [273.15, 373.15]),
        (["-10C", "-1"], [263.15, 272.15]),
        (None, None),
    ],
)
def test_temperature_normalization(temps_input, expected_kelvin):
    """Test normalization of simulation_temperature to Kelvin."""
    meta = SimulationMetadata(simulation_temperature=temps_input)
    assert meta.simulation_temperature == expected_kelvin


# -------------------------------------------------------------------
# Test software, molecules, forcefields creation
# -------------------------------------------------------------------
def test_structured_fields_creation():
    """Test that software, molecules, and forcefields can be created."""
    meta = SimulationMetadata(
        softwares=[{"name": "GROMACS", "version": "2023.1"}],
        molecules=[{"name": "H2O", "number_of_atoms": 3}],
        forcefields=[{"name": "AMBER", "version": "ff14SB"}],
    )

    assert meta.softwares[0].name == "GROMACS"
    assert meta.molecules[0].number_of_atoms == 3
    assert meta.forcefields[0].version == "ff14SB"


# -------------------------------------------------------------------
# Test invalid values in structured fields
# -------------------------------------------------------------------
def test_invalid_molecule_number_of_atoms():
    """Test that molecule number_of_atoms cannot be negative."""
    with pytest.raises(ValidationError):
        SimulationMetadata(
            molecules=[{"name": "H2O", "number_of_atoms": -1}]
        )


# -------------------------
# Test invalid simulation parameter type
# -------------------------
def test_invalid_simulation_value_type_raises_directly():
    """Test that non-numeric strings raise ValidationError."""
    with pytest.raises(ValidationError):
        SimulationMetadata(
            softwares=[{"name": "GROMACS", "version": "2023.1"}],
            molecules=[{"name": "H2O", "number_of_atoms": 3}],
            forcefields=[{"name": "AMBER", "version": "ff14SB"}],
            simulation_timestep_in_fs=["invalid"],  # <- invalide
            simulation_time=[1000.0],
            simulation_temperature=["300K"],
            number_of_total_atoms=3,
        )

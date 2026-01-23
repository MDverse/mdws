"""Pydantic data models used to validate simulation metadata from MD datasets."""

import re
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

DOI = Annotated[
    str,
    StringConstraints(pattern=r"^10\.\d{4,9}/[\w\-.]+$"),
]


class Molecule(BaseModel):
    """Represents a molecule in the simulation."""

    name: str = Field(..., description="Name or ID of the molecule.")
    number_of_atoms: int | None = Field(
        None, ge=0, description="Number of atoms in the molecule, if known."
    )


class ForceField(BaseModel):
    """Represents a forcefield used in the simulation."""

    name: str = Field(..., description="Name of the forcefield (e.g., AMBER).")
    version: str | None = Field(None, description="Version of the forcefield.")


class Software(BaseModel):
    """Represents the simulation software or tool used."""

    name: str = Field(
        ..., description="Molecular dynamics tool or software \
                            used (e.g. GROMACS, MDAnalysis)."
    )
    version: str | None = Field(None, description="Version of the software/tool.")


class SimulationMetadata(BaseModel):
    """Base Pydantic model for simulation-related metadata."""

    softwares: list[Software] | None = Field(
        None, description="List of molecular dynamics tool or software \
            used (e.g. GROMACS, MDAnalysis).",
    )
    number_of_total_atoms: int | None = Field(
        None,
        ge=0,  # equal or greater than zero
        description="Total number of atoms in the simulated system.",
    )
    molecules: list[Molecule] | None = Field(
        None, description="List of molecules in the system with \
            their number of atoms if known.",
    )
    forcefields: list[ForceField] | None = Field(
        None,
        description="List of Molecular dynamics forcefield model used (e.g. AMBER).",
    )
    simulation_timestep_in_fs: list[float] | None = Field(
        None, description="The time interval between new positions computation (in fs)."
    )
    simulation_time: list[str] | None = Field(
        None, description="The accumulated simulation times."
    )
    simulation_temperature: list[float] | None = Field(
        None, description="The temperature chosen for the simulations (in K)."
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("simulation_timestep_in_fs", "simulation_time", mode="before")
    def validate_positive_simulation_values(
        cls, v: list[str | float] | None  # noqa: N805
    ) -> list[str | float] | None:
        """Ensure simulation numeric parameters are strictly positive.

        Supported input types:
            - float (e.g. 0.0997, 1.2)
            - string containing a numeric value with optional units (e.g. "0.0997μs")

        Parameters
        ----------
        cls : type[BaseModel]
            The Pydantic model class being validated.
        v : list[str | float] | None
            Raw input simulation parameter value.

        Returns
        -------
        list[str | float] | None
            The validated value in the same structure as input, if all numeric values
            are strictly positive; otherwise raises ValueError.
        """
        if v is None:
            return None

        def check_positive(value: str | float | int):
            # Case 1: value is alrealdy numeric
            if isinstance(value, (int, float)):
                if value <= 0:
                    msg = "Simulation parameters must be strictly positive"
                    raise ValueError(msg)

            # Case 2: value is a string (e.g. "0.0997μs")
            elif isinstance(value, str):
                # Extract numeric part
                match = re.search(r"([-+]?\d*\.?\d+)", value)
                if not match or float(match.group(1)) <= 0:
                    msg = f"Invalid simulation parameter: {value}"
                    raise ValueError(msg)
            else:
                msg = f"Unsupported type for simulation parameter: {type(value)}"
                raise ValueError(msg)

        # Iterate over the possible values
        if isinstance(v, list):
            for item in v:
                check_positive(item)
            return v

        return v

    @field_validator("simulation_temperature", mode="before")
    def normalize_temperatures(
        cls, temperatures: list[str] | None  # noqa: N805
    ) -> list[float] | None:
        """
        Normalize temperatures to Kelvin.

        Supported format examples:
        - "300K" or "300" (assume Kelvin if no unit)
        - "27°C" or "27" (assume Celsius if ending with °C)

        Parameters
        ----------
        cls : type[BaseModel]
            The Pydantic model class being validated.
        temperatures : list[str] | None
            Raw temperature values.

        Returns
        -------
        list[float] | None
            Temperatures converted to Kelvin.

        Raises
        ------
        ValueError
            If a temperature string cannot be parsed
            as a number or has an invalid format.
        """
        if temperatures is None:
            return None

        kelvin_temperatures = []

        for temp_str in temperatures:
            temp_clean = str(temp_str).strip().lower()

            # Extract numeric part
            match = re.search(r"([-+]?\d*\.?\d+([eE][-+]?\d+)?)", temp_str)
            if match is None:
                msg = f"Cannot parse temperature: {temp_str}"
                raise ValueError(msg)
            numeric_value = float(match.group(1))

            # Convert Celsius to Kelvin
            if "c" in temp_clean or numeric_value < 0:
                kelvin_value = numeric_value + 273.15
            else:
                # Assume numeric-only or "K" input is already Kelvin
                kelvin_value = numeric_value

            kelvin_temperatures.append(kelvin_value)

        return kelvin_temperatures

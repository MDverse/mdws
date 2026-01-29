"""Pydantic data models used to validate simulation metadata from MD datasets."""

import re
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

DOI = Annotated[
    str,
    StringConstraints(pattern=r"^10\.\d{4,9}/[\w\-.]+$"),
]


class Molecule(BaseModel):
    """Molecule in a simulation."""

    name: str = Field(..., description="Name of the molecule.")
    number_of_atoms: int | None = Field(
        None, ge=0, description="Number of atoms in the molecule, if known."
    )
    formula: str | None = Field(
        None, description="Chemical formula of the molecule, if known."
    )
    number_of_molecules: int | None = Field(
        None,
        ge=0,
        description="Number of molecules of this type in the simulation, if known.",
    )


class ForceFieldModel(BaseModel):
    """Forcefield or Model used in a simulation."""

    name: str = Field(
        ...,
        description=(
            "Name of the forcefield or model. Examples:  AMBER, GROMOS, TIP3P..."
        ),
    )
    version: str | None = Field(None, description="Version of the forcefield or model.")


class Software(BaseModel):
    """Simulation software or tool used in a simulation."""

    name: str = Field(
        ...,
        description=(
            "Molecular dynamics tool or software used. "
            "Examples: GROMACS, NAMD, MDAnalysis."
        ),
    )
    version: str | None = Field(None, description="Version of the software/tool.")


class SimulationMetadata(BaseModel):
    """Base Pydantic model for MD simulation metadata.

    No field is required in this model; all are optional.
    """

    software: list[Software] | None = Field(
        None,
        description="List of molecular dynamics tool or software.",
    )
    total_number_of_atoms: int | None = Field(
        None,
        ge=0,  # equal or greater than zero
        description="Total number of atoms in the system.",
    )
    molecules: list[Molecule] | None = Field(
        None,
        description=("List of simulated molecules in the system."),
    )
    forcefields_models: list[ForceFieldModel] | None = Field(
        None,
        description="List of forcefields and models used.",
    )
    simulation_timesteps_in_fs: list[float] | None = Field(
        None, description="Simulation timestep (in fs)."
    )
    simulation_times: list[str] | None = Field(None, description="Simulation times.")
    simulation_temperatures_in_kelvin: list[float] | None = Field(
        None, description="Simulation temperatures (in K)."
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("simulation_timesteps_in_fs", "simulation_times", mode="before")
    @classmethod
    def validate_positive_simulation_values(
        cls,
        value: list[str | float] | None,
    ) -> list[str | float] | None:
        """Ensure simulation numeric parameters are strictly positive.

        Supported input types:
            - float (e.g. 0.0997, 1.2)
            - string containing a numeric value with optional units (e.g. "0.0997μs")

        Parameters
        ----------
        cls: SimulationMetadata
            The Pydantic model class being validated.
        value : list[str | float] | None
            Raw input simulation parameter value.

        Returns
        -------
        list[str | float] | None
            The validated value in the same structure as input, if all numeric values
            are strictly positive; otherwise raises ValueError.
        """
        if value is None:
            return None

        def check_positive(value: str | float | int):
            # Case 1: value is already numeric.
            if isinstance(value, (int, float)):
                if value <= 0:
                    msg = "Simulation parameters must be strictly positive"
                    raise ValueError(msg)
            # Case 2: value is a string (e.g. "0.0997μs").
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
        if isinstance(value, list):
            for item in value:
                check_positive(item)
            return value

        return value

    @field_validator("simulation_temperatures_in_kelvin", mode="before")
    @classmethod
    def normalize_temperatures(
        cls,
        temperatures: list[str] | None,
    ) -> list[float] | None:
        """
        Normalize temperatures to Kelvin.

        Examples of supported format:
        - "300K" or "300" (assume Kelvin if no unit)
        - "27°C" or "27" (assume Celsius if ending with °C)

        Parameters
        ----------
        cls: SimulationMetadata
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

        temperatures_in_kelvin = []
        for temp_str in temperatures:
            temp_clean = str(temp_str).strip().lower()
            # Extract the numeric part.
            match = re.search(r"([-+]?\d*\.?\d+([eE][-+]?\d+)?)", temp_clean)
            if match is None:
                msg = f"Cannot parse temperature: {temp_str}"
                raise ValueError(msg)
            numeric_value = float(match.group(1))
            # Convert Celsius to Kelvin.
            if "c" in temp_clean or numeric_value < 0:
                value_in_kelvin = numeric_value + 273.15
            else:
                # We assume we have Kelvin by default.
                value_in_kelvin = numeric_value
            temperatures_in_kelvin.append(value_in_kelvin)

        return temperatures_in_kelvin

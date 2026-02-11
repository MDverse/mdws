"""Pydantic data models used to validate simulation metadata from MD datasets."""

import re
from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from .enums import ExternalDatabaseName, MoleculeType

DOI = Annotated[
    str,
    StringConstraints(pattern=r"^10\.\d{4,9}/[\w\-.]+$"),
]


class ExternalIdentifier(BaseModel):
    """External database identifier."""

    # Ensure scraped metadata matches the expected schema exactly
    # and numbers are coerced to strings when needed.
    model_config = ConfigDict(extra="forbid", coerce_numbers_to_str=True)

    database_name: ExternalDatabaseName = Field(
        ...,
        description=(
            "Name of the external database. "
            "Allowed values are defined in ExternalDatabaseName enum. "
            "Examples: PDB, UNIPROT..."
        ),
    )
    identifier: str = Field(
        ...,
        min_length=1,
        description="Identifier in the external database.",
    )
    url: str | None = Field(
        None, min_length=1, description="Direct URL to the identifier into the database"
    )

    @model_validator(mode="after")
    def compute_url(self) -> "ExternalIdentifier":
        """Compute the URL for the external identifier.

        Parameters
        ----------
        self: ExternalIdentifier
            The model instance being validated, with all fields already validated.

        Returns
        -------
        ExternalIdentifier
            The model instance with the URL field computed if it was not provided.
        """
        if self.url is not None:
            return self

        if self.database_name == ExternalDatabaseName.PDB:
            self.url = f"https://www.rcsb.org/structure/{self.identifier}"
        elif self.database_name == ExternalDatabaseName.UNIPROT:
            self.url = f"https://www.uniprot.org/uniprotkb/{self.identifier}"

        return self


class Molecule(BaseModel):
    """Molecule in a simulation."""

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Name of the molecule.")
    type: MoleculeType | None = Field(
        None,
        description="Type of the molecule."
        "Allowed values in the MoleculeType enum. "
        "Examples: PROTEIN, ION, LIPID...",
    )
    number_of_molecules: int | None = Field(
        None,
        ge=0,
        description="Number of molecules of this type in the simulation.",
    )
    number_of_atoms: int | None = Field(
        None, ge=0, description="Number of atoms in the molecule."
    )
    formula: str | None = Field(None, description="Chemical formula of the molecule.")
    sequence: str | None = Field(
        None, description="Sequence of the molecule for protein and nucleic acid."
    )
    inchikey: str | None = Field(None, description="InChIKey of the molecule.")
    external_identifiers: list[ExternalIdentifier] | None = Field(
        None,
        description=("List of external database identifiers for this molecule."),
    )


class ForceFieldModel(BaseModel):
    """Forcefield or Model used in a simulation."""

    # Ensure scraped metadata matches the expected schema exactly
    # and version is coerced to string when needed.
    model_config = ConfigDict(extra="forbid", coerce_numbers_to_str=True)

    name: str = Field(
        ...,
        description=(
            "Name of the forcefield or model. Examples:  AMBER, GROMOS, TIP3P..."
        ),
    )
    version: str | None = Field(None, description="Version of the forcefield or model.")


class Software(BaseModel):
    """Simulation software or tool used in a simulation."""

    # Ensure scraped metadata matches the expected schema exactly
    # and version is coerced to string when needed.
    model_config = ConfigDict(extra="forbid", coerce_numbers_to_str=True)

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

    # Ensure scraped metadata matches the expected schema exactly.
    model_config = ConfigDict(extra="forbid")

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

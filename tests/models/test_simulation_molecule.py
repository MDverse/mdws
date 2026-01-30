"""Tests for the Pydantic molecule model."""

import pytest
from pydantic import ValidationError

from mdverse_scrapers.models.enums import ExternalDatabaseName
from mdverse_scrapers.models.simulation import (
    ExternalIdentifier,
    Molecule,
)


# -------------------------------------------------------------------
# Test invalid fields
# -------------------------------------------------------------------
def test_invalid_fields():
    """Test with a non-existing field."""
    with pytest.raises(ValidationError):
        Molecule(
            dummy_number=1000  # type: ignore
        )
    with pytest.raises(ValidationError):
        Molecule(
            dummy_str="dummy_value"  # type: ignore
        )


# -------------------------------------------------------------------
# Test invalid values
# -------------------------------------------------------------------
def test_invalid_number_of_atoms():
    """Test that number_of_atoms cannot be negative."""
    with pytest.raises(ValidationError):
        Molecule(name="H2O", number_of_atoms=-10, formula="H2O")


def test_invalid_number_of_molecules():
    """Test that number_of_molecules cannot be negative."""
    with pytest.raises(ValidationError):
        Molecule(name="H2O", number_of_molecules=-10, formula="H2O")


# -------------------------------------------------------------------
# Test external identifiers
# -------------------------------------------------------------------
@pytest.mark.parametrize(
    ("database_name", "identifier", "expected_identifier"),
    [
        (ExternalDatabaseName.PDB, "1K79", "1K79"),
        (ExternalDatabaseName.PDB, 1234, "1234"),
        (ExternalDatabaseName.UNIPROT, "P06213", "P06213"),
        (ExternalDatabaseName.UNIPROT, 123456, "123456"),
    ],
)
def test_external_identifiers_and_type_coercion(
    database_name, identifier, expected_identifier
):
    """Test external identifiers and type coercion."""
    # Valid external identifier
    external_identifier = ExternalIdentifier(
        database_name=database_name, identifier=identifier
    )
    assert external_identifier.database_name == database_name
    assert external_identifier.identifier == expected_identifier


def test_invalid_database_name_in_external_identifiers():
    """Test invalid database names."""
    # Invalid database name
    with pytest.raises(ValidationError):
        ExternalIdentifier(
            database_name="INVALID_DB",  # type: ignore
            identifier="1ABC",
        )
    with pytest.raises(AttributeError):
        ExternalIdentifier(
            database_name=ExternalDatabaseName.DUMMY,  # type: ignore
            identifier="1ABC",
        )

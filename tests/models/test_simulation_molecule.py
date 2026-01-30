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
            name="water",
            dummy_number=1000,  # type: ignore
        )
    with pytest.raises(ValidationError):
        Molecule(
            name="water",
            dummy_str="dummy_value",  # type: ignore
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
# Test ExternalIdentifier
# -------------------------------------------------------------------
@pytest.mark.parametrize(
    ("database_name", "identifier", "expected_identifier", "url"),
    [
        (
            ExternalDatabaseName.PDB,
            "1K79",
            "1K79",
            "https://www.rcsb.org/structure/1K79",
        ),
        (ExternalDatabaseName.PDB, 1234, "1234", None),
        (
            ExternalDatabaseName.UNIPROT,
            "P06213",
            "P06213",
            "https://www.uniprot.org/uniprotkb/P06213/entry",
        ),
        (ExternalDatabaseName.UNIPROT, 123456, "123456", None),
    ],
)
def test_external_identifier_creation(
    database_name, identifier, expected_identifier, url
):
    """Test creation of ExternalIdentifier instances."""
    external_identifier = ExternalIdentifier(
        database_name=database_name,
        identifier=identifier,
        url=url,
    )
    assert external_identifier.database_name == database_name
    assert external_identifier.identifier == expected_identifier
    assert external_identifier.url == url


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

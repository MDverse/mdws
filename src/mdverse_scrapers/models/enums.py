"""Enumerations for MDverse scrapers and models."""

from enum import StrEnum


class DataType(StrEnum):
    """Data types."""

    DATASETS = "datasets"
    FILES = "files"


class DatasetSourceName(StrEnum):
    """Molecular dynamics sources: data repositories and projects."""

    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"
    NMRLIPIDS = "nmrlipids"


class ExternalDatabaseName(StrEnum):
    """External database names."""

    PDB = "pdb"
    UNIPROT = "uniprot"

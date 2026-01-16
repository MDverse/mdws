"""Useful enumerations for MDverse scrapers."""

from enum import StrEnum


class DataType(StrEnum):
    """Supported data types."""

    DATASETS = "datasets"
    FILES = "files"


class DatasetRepositoryName(StrEnum):
    """Supported repositories from which molecular dynamics datasets are scraped."""

    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"


class DatasetProjectName(StrEnum):
    """Supported projects from which molecular dynamics datasets are scraped."""

    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"
    NMRLIPIDS = "nmrlipids"

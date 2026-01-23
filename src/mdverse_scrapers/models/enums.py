"""Enumerations for MDverse scrapers and models."""

from enum import StrEnum


class DataType(StrEnum):
    """Data types."""

    DATASETS = "datasets"
    FILES = "files"


class DatasetRepoProjectName(StrEnum):
    """Molecular dynamaics data repositories and projects."""

    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"
    NMRLIPIDS = "nmrlipids"

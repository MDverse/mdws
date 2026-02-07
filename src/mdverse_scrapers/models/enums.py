"""Enumerations for MDverse scrapers and models."""

from enum import StrEnum


class DataType(StrEnum):
    """Data types."""

    DATASETS = "datasets"
    FILES = "files"


class DatasetSourceName(StrEnum):
    """Molecular dynamics sources: data repositories and projects."""

    UNKNOWN = "unknown"
    ZENODO = "zenodo"
    FIGSHARE = "figshare"
    OSF = "osf"
    NOMAD = "nomad"
    ATLAS = "atlas"
    GPCRMD = "gpcrmd"
    NMRLIPIDS = "nmrlipids"
    MDDB = "mddb"
    MDPOSIT_INRIA_NODE = "mdposit_inria_node"
    MDPOSIT_MMB_NODE = "mdposit_mmb_node"


class ExternalDatabaseName(StrEnum):
    """External database names."""

    PDB = "pdb"
    UNIPROT = "uniprot"


class MoleculeType(StrEnum):
    """Common molecular types found in molecular dynamics simulations."""

    PROTEIN = "protein"
    NUCLEIC_ACID = "nucleic_acid"
    ION = "ion"
    LIPID = "lipid"
    CARBOHYDRATE = "carbohydrate"
    SOLVENT = "solvent"
    SMALL_MOLECULE = "small_molecule"

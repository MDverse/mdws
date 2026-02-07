"""Scrape molecular dynamics simulation datasets and files from the MDDB.

This script extracts molecular dynamics datasets managed by the
MDDB (Molecular Dynamics Data Bank) project
and the MDposit platform.
"""

import sys
from pathlib import Path
from urllib.parse import urlparse

import click
import httpx
import loguru

from ..core.logger import create_logger
from ..core.network import (
    HttpMethod,
    create_httpx_client,
    is_connection_to_server_working,
    make_http_request_with_retries,
)
from ..core.toolbox import print_statistics
from ..models.dataset import DatasetMetadata
from ..models.enums import DatasetSourceName, ExternalDatabaseName, MoleculeType
from ..models.scraper import ScraperContext
from ..models.simulation import ExternalIdentifier, ForceFieldModel, Molecule, Software
from ..models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    page_size: int = 50,
    logger: "loguru.Logger" = loguru.logger,
    scraper: ScraperContext | None = None,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the MDposit API.

    Within the MDposit terminology, datasets are referred to as "projects".

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    query_entry_point: str
        The entry point of the API request.
    page_size: int
        Number of entries to fetch per page.
    logger: "loguru.Logger"
        Logger for logging messages.
    scraper: ScraperContext | None
        Optional scraper context. When provided and running in debug mode,
        dataset scraping is intentionally stopped early to limit the amount
        of retrieved data.

    Returns
    -------
    list[dict]:
        List of MDposit entries metadata.
    """
    logger.info("Scraping molecular dynamics datasets from MDposit.")
    logger.info(f"Using batches of {page_size} datasets.")
    all_datasets = []
    # Start by requesting the first page to get total number of datasets.
    logger.info("Requesting first page to get total number of datasets...")
    params = {"limit": 10, "page": 1}
    response = make_http_request_with_retries(
        client,
        query_entry_point,
        method=HttpMethod.GET,
        params=params,
        timeout=60,
        delay_before_request=0.2,
    )
    if not response:
        logger.error("Failed to fetch data from MDposit API.")
        return all_datasets
    total_datasets = int(response.json().get("filteredCount", 0))
    logger.success(f"Found a total of {total_datasets:,} datasets in MDposit.")
    # Compute total number of pages to scrape based on total datasets and page size.
    page_total = total_datasets // page_size
    if total_datasets % page_size != 0:
        page_total += 1

    for page in range(1, page_total + 1):
        params = {"limit": page_size, "page": page}
        response = make_http_request_with_retries(
            client,
            query_entry_point,
            method=HttpMethod.GET,
            params=params,
            timeout=60,
            delay_before_request=0.2,
        )
        if not response:
            logger.error("Failed to fetch data from MDposit API.")
            logger.error("Jumping to next iteration.")
            continue

        response_json = response.json()
        datasets = response_json.get("projects", [])
        all_datasets.extend(datasets)

        logger.info(f"Scraped page {page}/{page_total} with {len(datasets)} datasets.")
        if total_datasets:
            logger.info(
                f"Scraped {len(all_datasets):,} datasets "
                f"({len(all_datasets):,}/{total_datasets:,}"
                f":{len(all_datasets) / total_datasets:.0%})"
            )
        logger.debug("First dataset metadata on this page:")
        logger.debug(datasets[0] if datasets else "No datasets on this page")

        if scraper and scraper.is_in_debug_mode and len(all_datasets) >= 100:
            logger.warning("Debug mode is ON: stopping after 100 datasets.")
            return all_datasets

    logger.success(f"Scraped {len(all_datasets):,} datasets in MDposit.")
    return all_datasets


def extract_software_and_version(
    dataset_metadata: dict, dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[Software] | None:
    """
    Extract software names and versions from the nested dataset dictionary.

    Example of dataset with no software:
    https://mdposit.mddbr.eu/api/rest/v1/projects/MD-A001R9

    Parameters
    ----------
    dataset_metadata: dict
        The dataset dictionary from which to extract molecules information.
    dataset_id: str
        Identifier of the dataset, used for logging.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[Software] | None
        A list of Software instances with `name` and `version` fields, None otherwise.
    """
    name = dataset_metadata.get("PROGRAM")
    version = dataset_metadata.get("VERSION")
    if not name:
        logger.warning("No software found for dataset")
        return None
    return [Software(name=name.strip(), version=version)]


def extract_forcefield_or_model_and_version(
    dataset_metadata: dict, dataset_id: str, logger: "loguru.Logger" = loguru.logger
) -> list[ForceFieldModel] | None:
    """
    Extract forcefield or model names and versions from the nested dataset dictionary.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset dictionary from which to extract molecules information.
    dataset_id: str
        Identifier of the dataset entry, used for logging.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[ForceFieldModel] | None
        A list of forcefield or model instances with `name` and `version` fields,
        None otherwise.
    """
    forcefields_and_models = []
    # Add forcefield names.
    forcefields = dataset_metadata.get("FF")
    if forcefields:
        for forcefield in forcefields:
            if isinstance(forcefield, str):
                forcefields_and_models.append(ForceFieldModel(name=forcefield.strip()))
                logger.debug(f"Found forcefield/model: {forcefield.strip()}")
    # Add water model.
    water_model = dataset_metadata.get("WAT", "")
    if water_model:
        forcefields_and_models.append(ForceFieldModel(name=water_model.strip()))
        logger.debug(f"Found water model: {water_model.strip()}")
    # Print summary of extracted forcefields and models.
    if forcefields_and_models:
        logger.info(f"Found {len(forcefields_and_models)} forcefield(s) or model(s)")
    else:
        logger.warning("No forcefield or model found")
        return None
    return forcefields_and_models


def fetch_uniprot_protein_name(
    client: httpx.Client,
    uniprot_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> str:
    """
    Retrieve protein name from UniProt API.

    Parameters
    ----------
    client: httpx.Client
        HTTP client used to perform the request.
    uniprot_id: str
        UniProt accession identifier.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    str
        Protein full name if available, None otherwise.
    """
    logger.info(f"Fetching protein name for UniProt ID: {uniprot_id}")
    if uniprot_id in ("noref", "notfound"):
        logger.warning(f"Cannot fetch protein name for UniProt ID '{uniprot_id}'")
        return "Unknown protein"
    # Defaut value for protein name:
    default_protein_name = f"Protein {uniprot_id}"
    response = make_http_request_with_retries(
        client,
        f"https://rest.uniprot.org/uniprotkb/{uniprot_id}",
        method=HttpMethod.GET,
        timeout=30,
        delay_before_request=0.1,
    )
    if not response:
        logger.error(f"Failed to query the UniProt API for ID {uniprot_id}")
        return default_protein_name
    protein_name = (
        response.json()
        .get("proteinDescription", {})
        .get("recommendedName", {})
        .get("fullName", {})
        .get("value")
    )
    if protein_name:
        logger.success(f"Retrieved protein name for UniProt ID {uniprot_id}:")
        logger.success(protein_name)
        return protein_name
    else:
        logger.warning(
            f"Protein name not found in UniProt API response for ID {uniprot_id}"
        )
        return default_protein_name


def extract_proteins(  # noqa: C901
    pdb_identifiers: list[ExternalIdentifier],
    uniprot_identifiers: list[str],
    protein_sequences: list[str],
    client: httpx.Client,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list:
    """Extract proteins from dataset metadata.

    Parameters
    ----------
    pdb_identifiers: list[ExternalIdentifier]
        List of PDB identifiers to associate with the proteins.
    uniprot_identifiers: list[str]
        List of UniProt accessions.
        to associate with the proteins.
    protein_sequences: list[str]
        List of protein sequences.
    client: httpx.Client
        The HTTP client used for making requests.
    dataset_id: str
        The ID of the dataset being processed, used for logging.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list
        A list of extracted proteins or empty list.
    """
    molecules = []
    # Case 1:
    # We have no protein sequences but no UniProt identifiers.
    if not protein_sequences and not uniprot_identifiers:
        logger.info(
            "No protein sequences or UniProt identifiers found "
            f"in dataset {dataset_id}."
        )
        if pdb_identifiers:
            molecules.append(
                Molecule(
                    name="Protein",
                    type=MoleculeType.PROTEIN,
                    sequence=None,
                    external_identifiers=pdb_identifiers,
                )
            )
        return molecules
    # Case 2:
    # We have protein sequences but no UniProt identifiers.
    if protein_sequences and not uniprot_identifiers:
        logger.warning(
            "Protein sequences found but no UniProt identifier "
            f"in dataset {dataset_id}."
        )
        for sequence in protein_sequences:
            molecules.append(
                Molecule(
                    name="Protein",
                    type=MoleculeType.PROTEIN,
                    sequence=sequence,
                    external_identifiers=pdb_identifiers,
                )
            )
        return molecules
    # Case 3:
    # We have UniProt identifiers but no protein sequences.
    if uniprot_identifiers and not protein_sequences:
        logger.warning(
            "UniProt identifiers found but no protein sequence "
            f"in dataset {dataset_id}."
        )
        for identifier in uniprot_identifiers:
            external = ExternalIdentifier(
                database_name=ExternalDatabaseName.UNIPROT, identifier=identifier
            )
            protein_name = fetch_uniprot_protein_name(client, identifier, logger=logger)
            molecules.append(
                Molecule(
                    name=protein_name,
                    type=MoleculeType.PROTEIN,
                    sequence=None,
                    external_identifiers=[external, *pdb_identifiers],
                )
            )
        return molecules
    # Case 4:
    # We have one UniProt identifier and several protein sequences,
    # we assume all protein sequences are associated with the same UniProt identifier.
    if (len(uniprot_identifiers) == 1) and (len(protein_sequences) > 1):
        external = ExternalIdentifier(
            database_name=ExternalDatabaseName.UNIPROT,
            identifier=uniprot_identifiers[0],
        )
        protein_name = fetch_uniprot_protein_name(
            client, uniprot_identifiers[0], logger=logger
        )
        for sequence in protein_sequences:
            molecules.append(
                Molecule(
                    name=protein_name,
                    type=MoleculeType.PROTEIN,
                    sequence=sequence,
                    external_identifiers=[external, *pdb_identifiers],
                )
            )
        return molecules
    # Case 5:
    # We have more than one UniProt identifiers and several protein sequences,
    # but their numbers do not match.
    if len(uniprot_identifiers) != len(protein_sequences):
        logger.warning(
            f"Number of UniProt identifiers ({len(uniprot_identifiers)}) does not "
            f"match number of protein sequences ({len(protein_sequences)})"
        )
        if pdb_identifiers:
            molecules.append(
                Molecule(
                    name="Unknown protein",
                    type=MoleculeType.PROTEIN,
                    external_identifiers=pdb_identifiers,
                )
            )
        return molecules
    # Case 6:
    # We have UniProt identifiers and protein sequences,
    # and their numbers match.
    for identifier, sequence in zip(
        uniprot_identifiers, protein_sequences, strict=True
    ):
        external = ExternalIdentifier(
            database_name=ExternalDatabaseName.UNIPROT, identifier=identifier
        )
        protein_name = fetch_uniprot_protein_name(client, identifier, logger=logger)
        molecules.append(
            Molecule(
                name=protein_name,
                type=MoleculeType.PROTEIN,
                sequence=sequence,
                external_identifiers=[external, *pdb_identifiers],
            )
        )
    return molecules


def extract_nucleic_acids(
    pdb_identifiers: list[ExternalIdentifier],
    nucleic_acid_sequences: list[str],
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list:
    """Extract nucleic acids from dataset metadata.

    Parameters
    ----------
    pdb_identifiers: list[ExternalIdentifier]
        List of PDB identifiers to associate with the proteins.
    nucleic_acid_sequences: list[str]
        List of nucleic acid sequences.
    dataset_id: str
        The ID of the dataset being processed, used for logging.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list
        A list of extracted nucleic acids.
    """
    molecules = []
    for sequence in nucleic_acid_sequences:
        molecules.append(
            Molecule(
                name="Nucleic acid",
                type=MoleculeType.NUCLEIC_ACID,
                sequence=sequence,
                external_identifiers=pdb_identifiers,
            )
        )
    return molecules


def extract_small_molecules(
    dataset_metadata: dict,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list:
    """Extract small molecules (lipids, solvents, ions) from dataset metadata.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset metadata containing information about the molecules.
    dataset_id: str
        The ID of the dataset being processed, used for logging.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list
        A list of extracted small molecules or an empty list.
    """
    molecules = []
    name_type_mapping = {
        "SOL": MoleculeType.SOLVENT,
        "NA": MoleculeType.ION,
        "CL": MoleculeType.ION,
    }
    for name, mol_type in name_type_mapping.items():
        count = dataset_metadata.get(name, 0)
        if isinstance(count, int) and count > 0:
            molecules.append(
                Molecule(
                    name=name,
                    type=mol_type,
                    number_of_molecules=count,
                )
            )
    # Get InChIKey for small molecules if available.
    inchikeys = dataset_metadata.get("INCHIKEYS")
    if inchikeys and isinstance(inchikeys, list):
        for inchikey in inchikeys:
            molecules.append(
                Molecule(
                    name="Small molecule",
                    type=MoleculeType.SMALL_MOLECULE,
                    inchikey=inchikey,
                )
            )
    return molecules


def extract_molecules(
    dataset_metadata: dict,
    dataset_id: str,
    client: httpx.Client,
    logger: "loguru.Logger" = loguru.logger,
) -> list[Molecule] | None:
    """Coordinator function to extract all molecule types from dataset metadata.

    Parameters
    ----------
    dataset_metadata: dict
        The dataset metadata containing information about the molecules.
    dataset_id: str
        The ID of the dataset being processed.
    client: httpx.Client
        The HTTP client used for making requests.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[Molecule] | None
        A list of extracted molecules or None if no molecules were found.
    """
    molecules = []
    # Add PDB identifiers as external identifiers.
    pdb_identifiers = []
    for pdb in dataset_metadata.get("PDBIDS", []):
        external = ExternalIdentifier(
            database_name=ExternalDatabaseName.PDB, identifier=pdb
        )
        pdb_identifiers.append(external)
    # Add UniProt identifiers and protein sequence.
    # Example with no PDBIDS, no PROTSEQ and no REFERENCES:
    # https://mdposit.mddbr.eu/api/rest/v1/projects/MD-A001M3
    proteins = extract_proteins(
        pdb_identifiers,
        dataset_metadata.get("REFERENCES", []),
        dataset_metadata.get("PROTSEQ", []),
        client,
        dataset_id,
        logger=logger,
    )
    if proteins:
        logger.info(f"Found {len(proteins)} protein(s)")
        molecules.extend(proteins)
    # Add nucleic acids.
    # See for instance: https://mdposit.mddbr.eu/api/rest/v1/projects/MD-A001M3
    nucleic_acids = extract_nucleic_acids(
        pdb_identifiers, dataset_metadata.get("NUCLSEQ", []), dataset_id, logger=logger
    )
    if nucleic_acids:
        logger.info(f"Found {len(nucleic_acids)} nucleic acid(s)")
        molecules.extend(nucleic_acids)
    # Finally extract small molecules like lipids, solvents and ions.
    small_molecules = extract_small_molecules(
        dataset_metadata, dataset_id, logger=logger
    )
    if small_molecules:
        logger.info(f"Found {len(small_molecules)} small molecule(s)")
        molecules.extend(small_molecules)
    # Print summary of extracted molecules.
    if molecules:
        logger.info(
            f"Found a total of {len(molecules)} molecule(s) in dataset {dataset_id}"
        )
    else:
        logger.warning(f"No molecules found in dataset {dataset_id}")
        return None
    return molecules


def extract_datasets_metadata(
    datasets: list[dict],
    client: httpx.Client,
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[list[dict], dict]:
    """
    Extract relevant metadata from raw MDposit datasets metadata.

    Parameters
    ----------
    datasets: list[dict]
        List of raw MDposit datasets metadata.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    dict
        Dictionnary for replicas by dataset.
    """
    datasets_metadata = []
    replicas = {}
    for dataset in datasets:
        # Get the dataset id
        dataset_id = str(dataset.get("accession"))
        logger.info("-" * 50)
        logger.info(f"Extracting metadata for dataset: {dataset_id}")
        logger.debug(f"https://mdposit.mddbr.eu/api/rest/v1/projects/{dataset_id}")
        # Extract node name.
        node_name = dataset.get("node", "")
        node_name_full = f"mdposit_{dataset.get('node', '')}_node"
        # Create the dataset url depending on the node.
        dataset_repository_name = DatasetSourceName.UNKNOWN
        dataset_id_in_repository = ""
        dataset_url_in_repository = ""
        if node_name_full == DatasetSourceName.MDPOSIT_MMB_NODE:
            dataset_repository_name = DatasetSourceName.MDPOSIT_MMB_NODE
            dataset_id_in_repository = str(dataset.get("local"))
            dataset_url_in_repository = (
                f"https://mmb.mddbr.eu/#/id/{dataset_id_in_repository}/overview"
            )
        elif (
            (node_name_full == DatasetSourceName.MDPOSIT_INRIA_NODE)
            or (node_name == "inr")  # For compatibility with error in database
        ):
            dataset_repository_name = DatasetSourceName.MDPOSIT_INRIA_NODE
            dataset_id_in_repository = str(dataset.get("local"))
            dataset_url_in_repository = (
                f"https://dynarepo.inria.fr/#/id/{dataset_id_in_repository}/overview"
            )
            if node_name == "inr":
                logger.warning(
                    f"Dataset {dataset_id} is associated with node 'inr', "
                    "which seems to be an error in the database"
                )
                logger.warning(
                    f"Using node name '{DatasetSourceName.MDPOSIT_INRIA_NODE}'"
                )
        else:
            logger.error(f"Unknown MDDB node '{node_name}' for dataset {dataset_id}")
            logger.error("Skipping dataset")
            continue

        simulation_metadata = dataset.get("metadata", {})
        citations = simulation_metadata.get("CITATION")
        external_links = [citations] if citations else None
        authors = simulation_metadata.get("AUTHORS")
        author_names = None
        if isinstance(authors, list):
            author_names = authors
        elif isinstance(authors, str):
            author_names = [authors.strip()]
        metadata = {
            "dataset_repository_name": dataset_repository_name,
            "dataset_id_in_repository": dataset_id_in_repository,
            "dataset_url_in_repository": dataset_url_in_repository,
            "dataset_project_name": DatasetSourceName.MDDB,
            "dataset_id_in_project": dataset_id,
            "dataset_url_in_project": f"https://mdposit.mddbr.eu/#/id/{dataset_id}/overview",
            "external_links": external_links,
            "title": simulation_metadata.get("NAME"),
            "date_created": dataset.get("creationDate"),
            "date_last_updated": dataset.get("updateDate"),
            "number_of_files": len(dataset.get("files", [])),
            "author_names": author_names,
            "license": simulation_metadata.get("LICENSE"),
            "description": simulation_metadata.get("DESCRIPTION"),
            "total_number_of_atoms": simulation_metadata.get("mdAtoms"),
        }
        # Extract simulation metadata if available.
        # Software names with their versions.
        metadata["software"] = extract_software_and_version(
            simulation_metadata, dataset_id, logger=logger
        )
        # Forcefield and model names with their versions.
        metadata["forcefields_models"] = extract_forcefield_or_model_and_version(
            simulation_metadata, dataset_id, logger=logger
        )
        # Molecules with their nb of atoms and number total of atoms.
        metadata["molecules"] = extract_molecules(
            simulation_metadata, dataset_id, client, logger=logger
        )
        # Time step in fs.
        time_step = simulation_metadata.get("TIMESTEP")
        metadata["simulation_timesteps_in_fs"] = [time_step] if time_step else None
        # Temperatures in kelvin.
        temperature = simulation_metadata.get("TEMP")
        if temperature and isinstance(temperature, (int, float)):
            metadata["simulation_temperatures_in_kelvin"] = [temperature]
            logger.debug(f"Found simulation temperature: {temperature} K")
        else:
            logger.warning("No simulation temperature found")
        # Extract replicas.
        replica_list = dataset.get("mds")
        if replica_list:
            replicas[dataset_id] = replica_list
        # Append extracted metadata.
        datasets_metadata.append(metadata)
        logger.info(
            "Extracted metadata for "
            f"{len(datasets_metadata):,}/{len(datasets):,} datasets "
            f"({len(datasets_metadata) / len(datasets):.0%})"
        )
    return datasets_metadata, replicas


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets_metadata: list[DatasetMetadata],
    datasets_replicas: dict,
    node_base_url: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """Scrape files metadata for all datasets in MDposit API.

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    datasets_metadata: list[DatasetMetadata]
        List of datasets to scrape files metadata for.
    datasets_replicas: dict
        Dictionnary for replicas by dataset.
    node_base_url: str
        Base url of the specific node of MDposit API.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of files metadata dictionaries.
    """
    all_files_metadata = []
    for dataset_count, dataset in enumerate(datasets_metadata, start=1):
        logger.info("-" * 50)
        dataset_id = dataset.dataset_id_in_project
        for replica_id, replica_name in enumerate(
            datasets_replicas.get(dataset_id, []), start=1
        ):
            logger.info(f"Scraping files for dataset: {dataset_id} / {replica_name}")
            response = make_http_request_with_retries(
                client,
                url=f"{node_base_url}/projects/{dataset_id}.{replica_id}/filenotes",
                method=HttpMethod.GET,
                timeout=60,
                delay_before_request=0.1,
                logger=logger,
            )
            if not response:
                logger.error("Failed to fetch files metadata")
                continue
            raw_files_metadata = response.json()
            # Extract relevant files metadata.
            logger.info(
                f"Extracting files metadata for dataset: {dataset_id} / {replica_name}"
            )
            # We integrate replica name and id to distinguish files
            # from different replicas of the same dataset,
            # as they usually have the same names.
            files_metadata = extract_files_metadata(
                raw_files_metadata,
                node_base_url,
                dataset,
                replica_id,
                replica_name,
                logger=logger,
            )
            all_files_metadata += files_metadata
            # Normalize files metadata with pydantic model (FileMetadata)
            logger.success(f"Total number of files found: {len(all_files_metadata):,}")
        logger.success(
            "Extracted files metadata for "
            f"{dataset_count:,}/{len(datasets_metadata):,} "
            f"({dataset_count / len(datasets_metadata):.0%}) datasets"
        )
    return all_files_metadata


def extract_files_metadata(
    raw_metadata: list[dict],
    node_base_url: str,
    dataset: DatasetMetadata,
    replica_id: int,
    replica_name: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Extract relevant metadata from raw MDposit files metadata.

    Parameters
    ----------
    raw_metadata: list[dict]
        Raw files metadata.
    node_base_url: str
        The unique identifier of the dataset in MDDB.
    dataset: DatasetMetadata
        Normalized dataset to get files metadata for.
    replica_id: int
        Identifer of the corresponding replica associated with the files.
    replica_name: str
        The name of the corresponding replica associated with the files.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of select files metadata.
    """
    logger.info("Extracting files metadata...")
    files_metadata = []
    for mdposit_file in raw_metadata:
        dataset_id = dataset.dataset_id_in_repository
        file_name = Path(mdposit_file.get("filename", ""))
        # Extract base url from dataset url.
        base_url = urlparse(dataset.dataset_url_in_repository).netloc
        file_path_url = f"https://{base_url}/api/rest/current/projects/{dataset_id}.{replica_id}/files/{file_name}"
        file_metadata = {
            "dataset_repository_name": dataset.dataset_repository_name,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset.dataset_url_in_repository,
            "file_name": f"{replica_name.replace(' ', '_')}/{file_name}",
            "file_size_in_bytes": mdposit_file.get("length", None),
            "file_md5": mdposit_file.get("md5", None),
            "file_url_in_repository": file_path_url,
        }
        files_metadata.append(file_metadata)
    logger.info(f"Extracted metadata for {len(files_metadata)} files")
    return files_metadata


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(output_dir_path: Path, *, is_in_debug_mode: bool = False) -> None:
    """Scrape molecular dynamics datasets and files from MDDB."""
    # Create HTTPX client
    client = create_httpx_client()

    data_source_name = DatasetSourceName.MDDB
    base_url = "https://mdposit.mddbr.eu/api/rest/v1"
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=data_source_name,
        output_dir_path=output_dir_path,
        is_in_debug_mode=is_in_debug_mode,
    )
    # Create logger.
    level = "DEBUG" if scraper.is_in_debug_mode else "INFO"
    logger = create_logger(logpath=scraper.log_file_path, level=level)
    # Print scraper configuration.
    logger.debug(scraper.model_dump_json(indent=4, exclude={"token"}))
    logger.info(f"Starting {data_source_name.name} data scraping...")
    # Check connection to the API
    if is_connection_to_server_working(
        client, f"{base_url}/projects/summary", logger=logger
    ):
        logger.success(f"Connection to {data_source_name} API successful!")
    else:
        logger.critical(f"Connection to {data_source_name} API failed.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Scrape the datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client,
        query_entry_point=f"{base_url}/projects",
        logger=logger,
        scraper=scraper,
    )
    if not datasets_raw_metadata:
        logger.critical(f"No datasets found in {data_source_name}.")
        logger.critical("Aborting.")
        sys.exit(1)

    # Extract datasets metadata.
    datasets_selected_metadata, replicas = extract_datasets_metadata(
        datasets_raw_metadata, client, logger=logger
    )
    # Validate datasets metadata with the DatasetMetadata Pydantic model.
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata, logger=logger
    )
    # Save datasets metadata to parquet file.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_normalized_metadata,
        logger=logger,
    )
    # Output first dataset metadata for debugging purposes.
    logger.debug("First dataset metadata:")
    logger.debug(datasets_normalized_metadata[0])
    # Scrape MDDB files metadata.
    files_metadata = scrape_files_for_all_datasets(
        client,
        datasets_normalized_metadata,
        replicas,
        base_url,
        logger=logger,
    )
    # Validate MDDB files metadata with the FileMetadata Pydantic model.
    files_normalized_metadata = normalize_files_metadata(files_metadata, logger=logger)
    # Save files metadata to parquet file.
    scraper.number_of_files_scraped = export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_normalized_metadata,
        logger=logger,
    )
    # Output first file metadata for debugging purposes.
    logger.debug("First file metadata:")
    logger.debug(files_normalized_metadata[0])
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()

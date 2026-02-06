"""Scrape molecular dynamics simulation datasets and files from the MDDB.

This script extracts molecular dynamics datasets produced within the
MDDB (Molecular Dynamics Data Bank) project, which is distributed across
two nodes:

- MDPOSIT MMB node (https://mmb-dev.mddbr.eu/#/browse)
- MDPOSIT INRIA node https://dynarepo.inria.fr/#/
"""

import sys
from pathlib import Path
from typing import Any

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

MDDB_REPOSITORIES = {
    DatasetSourceName.MDPOSIT_MMB_NODE: "https://mmb-dev.mddbr.eu/api/rest/v1",
    DatasetSourceName.MDPOSIT_INRIA_NODE: "https://inria.mddbr.eu/api/rest/v1",
}


def scrape_all_datasets(
    client: httpx.Client,
    query_entry_point: str,
    node_name: DatasetSourceName,
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
    node_name: DatasetSourceName
        MDDB node name for logging.
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
        A list of MDposit entries.
    """
    logger.info(f"Scraping molecular dynamics datasets from {node_name}.")
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
    logger.success(f"Found a total of {total_datasets:,} datasets in {node_name}.")
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
                f"Scraped {len(all_datasets)} datasets "
                f"({len(all_datasets):,}/{total_datasets:,} "
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
    try:
        name = dataset_metadata.get("PROGRAM")
        version = dataset_metadata.get("VERSION")
        if not name:
            return None
        return [Software(name=name, version=str(version))]
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Error parsing software info for dataset {dataset_id}: {e}")
        return None


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
    ffm_names = []
    try:
        # Adding
        ffm_names.extend(dataset_metadata.get("FF") or [])
        # Adding the water model.
        # Example: TIP3P.
        water_model = dataset_metadata.get("WAT")
        if water_model:
            ffm_names.append(water_model)
        return [ForceFieldModel(name=ffm_name) for ffm_name in ffm_names if ffm_name]
    except (ValueError, KeyError) as e:
        logger.warning(
            f"Error parsing forcefield or model info for dataset {dataset_id}: {e}"
        )
        return None


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
    if uniprot_id == "noref":
        logger.warning("UniProt ID is 'noref', cannot fetch protein name.")
        return "Unknow protein"
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
        logger.error(f"Failed to fetch data from UniProt API for ID {uniprot_id}.")
        return default_protein_name
    protein_name = (
        response.json()
        .get("proteinDescription", {})
        .get("recommendedName", {})
        .get("fullName", {})
        .get("value")
    )
    if protein_name:
        logger.success(
            f"Retrieved protein name for UniProt ID {uniprot_id}: {protein_name}"
        )
        return protein_name
    return default_protein_name


def extract_proteins(
    pdb_ids: list[ExternalIdentifier],
    references: list[str],
    prot_seqs: list[str],
    prot_atoms: int | None,
    client: httpx.Client,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[Molecule]:
    """Extract proteins from dataset metadata.

    Parameters
    ----------
    pdb_ids: list[ExternalIdentifier]
        List of PDB identifiers to associate with the proteins.
    references: list[str]
        List of reference identifiers (e.g., UniProt accessions)
        to associate with the proteins.
    prot_seqs: list[str]
        List of protein sequences.
    prot_atoms: int | None
        Total number of atoms in the protein.
    client: httpx.Client
        The HTTP client used for making requests.
    dataset_id: str
        The ID of the dataset being processed, used for logging.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[Molecule]
        A list of extracted proteins.
    """
    molecules = []
    for counter, seq in enumerate(prot_seqs):
        external_ids = list(pdb_ids)
        prot_name = f"Protein {counter + 1}"
        try:
            uniprot_id = references[counter] if counter < len(references) else None
            if uniprot_id:
                external_ids.append(
                    ExternalIdentifier(
                        database_name=ExternalDatabaseName.UNIPROT,
                        identifier=uniprot_id,
                    )
                )
                prot_name = fetch_uniprot_protein_name(
                    client, uniprot_id, logger=logger
                )
        except (TypeError, ValueError) as exc:
            logger.warning(
                f"Skipping protein {counter + 1} in dataset {dataset_id} due to "
                f"{type(exc).__name__}: {exc}"
            )
        molecules.append(
            Molecule(
                name=prot_name,
                type=MoleculeType.PROTEIN,
                sequence=seq,
                number_of_atoms=prot_atoms if len(prot_seqs) == 1 else None,
                external_identifiers=external_ids,
            )
        )
    return molecules


def extract_nucleic_acids(
    pdb_ids: list[ExternalIdentifier],
    nucl_seqs: list[str],
    nucl_atoms: int | None,
    dataset_id: str,
    logger: "loguru.Logger",
) -> list[Molecule]:
    """Extract nucleic acids from dataset metadata.

    Parameters
    ----------
    pdb_ids: list[ExternalIdentifier]
        List of PDB identifiers to associate with the nucleic acids.
    nucl_seqs: list[str]
        List of nucleic acid sequences.
    nucl_atoms: int
        Total number of atoms in the nucleic acids.
    dataset_id: str
        The ID of the dataset being processed, used for logging.
    logger: loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[Molecule]
        A list of extracted nucleic acids.
    """
    molecules = []
    for i, seq in enumerate(nucl_seqs):
        try:
            molecules.append(
                Molecule(
                    name=f"Nucleic Acid {i + 1}",
                    type=MoleculeType.NUCLEIC_ACID,
                    sequence=seq,
                    number_of_atoms=nucl_atoms if len(nucl_seqs) == 1 else None,
                    external_identifiers=pdb_ids,
                )
            )
        except (TypeError, ValueError) as exc:
            logger.warning(
                f"Skipping nucleic acid {i + 1} in dataset {dataset_id} "
                f"due to {type(exc).__name__}: {exc}"
            )
    return molecules


def extract_small_molecules(
    dataset_metadata: dict,
    dataset_id: str,
    logger: "loguru.Logger",
) -> list[Molecule]:
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
    list[Molecule]
        A list of extracted small molecules.
    """
    molecules = []
    species_type_map = {
        "DPPC": MoleculeType.LIPID,
        "SOL": MoleculeType.SOLVENT,
        "NA": MoleculeType.ION,
        "CL": MoleculeType.ION,
    }
    for species, mol_type in species_type_map.items():
        try:
            count = dataset_metadata.get(species, 0)
            if isinstance(count, int) and count > 0:
                molecules.append(
                    Molecule(
                        name=species,
                        type=mol_type,
                        number_of_molecules=count,
                    )
                )
        except (TypeError, ValueError) as exc:
            logger.warning(
                f"Skipping small molecule {species} in dataset {dataset_id} "
                f"due to {type(exc).__name__}: {exc}"
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
    molecules: list[Molecule] = []

    # Normalize common fields
    pdbs = dataset_metadata.get("PDBIDS") or []
    references = dataset_metadata.get("REFERENCES") or []
    prot_seqs = dataset_metadata.get("PROTSEQ") or []
    prot_atoms = dataset_metadata.get("PROTATS")
    nucl_seqs = dataset_metadata.get("NUCLSEQ") or []
    nucl_atoms = dataset_metadata.get("DNAATS", 0) + (dataset_metadata.get("RNAATS", 0))

    # Pre-create PDB identifiers
    pdb_ids = [
        ExternalIdentifier(database_name=ExternalDatabaseName.PDB, identifier=pdb_id)
        for pdb_id in pdbs
    ]

    # Extract proteins first
    molecules.extend(
        extract_proteins(
            pdb_ids,
            references,
            prot_seqs,
            prot_atoms,
            client,
            dataset_id,
            logger=logger,
        )
    )
    # Then extract nucleic acids
    molecules.extend(
        extract_nucleic_acids(pdb_ids, nucl_seqs, nucl_atoms, dataset_id, logger)
    )
    # Finally extract small molecules like lipids, solvents and ions.
    molecules.extend(extract_small_molecules(dataset_metadata, dataset_id, logger))

    if not molecules:
        logger.warning(f"No molecules found in dataset {dataset_id}.")
        return None

    return molecules


def extract_datasets_metadata(
    datasets: list[dict[str, Any]],
    node_name: DatasetSourceName,
    client: "httpx.Client",
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict[str, Any]]:
    """
    Extract relevant metadata from raw MDposit datasets metadata.

    Parameters
    ----------
    datasets: List[Dict[str, Any]]
        List of raw MDposit datasets metadata.
    node_name: DatasetSourceName
        MDDB node name for the dataset url.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    list[dict[str, Any]]
        List of dataset metadata dictionaries.
    """
    datasets_metadata = []
    for dataset in datasets:
        # Get the dataset id
        dataset_id = str(dataset.get("accession"))
        logger.info(f"Extracting metadata for dataset: {dataset_id}")
        # Create the dataset url depending on the node
        if node_name is DatasetSourceName.MDPOSIT_MMB_NODE:
            dataset_url = f"https://mmb-dev.mddbr.eu/#/id/{dataset_id}/overview"
        elif node_name is DatasetSourceName.MDPOSIT_INRIA_NODE:
            dataset_url = f"https://dynarepo.inria.fr/#/id/{dataset_id}/overview"
        else:
            logger.warning(
                f"Unknown MDDB node '{node_name}'."
                f"Cannot build entry URL for dataset {dataset_id}."
            )

        dataset_metadata = dataset.get("metadata", {})
        links = dataset_metadata.get("CITATION")
        links_list = [links] if links else None
        a = dataset_metadata.get("AUTHORS")
        author_names = a if isinstance(a, list) else [a] if a else None
        metadata = {
            "dataset_repository_name": node_name.value,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset_url,
            "dataset_project_name": DatasetSourceName.MDDB,
            "external_links": links_list,
            "title": dataset_metadata.get("NAME"),
            "date_created": dataset.get("creationDate"),
            "date_last_updated": dataset.get("updateDate"),
            "number_of_files": len(dataset.get("files", [])),
            "author_names": author_names,
            "license": dataset_metadata.get("LICENSE"),
            "description": dataset_metadata.get("DESCRIPTION"),
            "total_number_of_atoms": dataset_metadata.get("mdAtoms"),
        }
        # Extract simulation metadata if available.
        # Software names with their versions.
        metadata["software"] = extract_software_and_version(
            dataset_metadata, dataset_id, logger
        )
        # Forcefield and model names with their versions.
        metadata["forcefields_models"] = extract_forcefield_or_model_and_version(
            dataset_metadata, dataset_id, logger
        )
        # Molecules with their nb of atoms and number total of atoms.
        metadata["molecules"] = extract_molecules(
            dataset_metadata, dataset_id, client, logger
        )
        # Time step in fs.
        time_step = dataset_metadata.get("TIMESTEP")
        metadata["simulation_timesteps_in_fs"] = [time_step] if time_step else None
        # Temperatures in kelvin
        temperature = dataset_metadata.get("TEMP")
        metadata["simulation_temperatures_in_kelvin"] = (
            [temperature] if temperature else None
        )
        datasets_metadata.append(metadata)
        logger.info(
            f"Scraped metadata for {len(datasets_metadata)} datasets "
            f"({len(datasets_metadata):,}/{len(datasets):,}"
            f":{len(datasets_metadata) / len(datasets):.0%})"
        )
    return datasets_metadata


def scrape_files_for_one_dataset(
    client: httpx.Client,
    url: str,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """
    Scrape files metadata for a given MDposit dataset.

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    url: str
        The URL endpoint.
    dataset_id: str
        The unique identifier of the dataset in MDposit.
    logger: "loguru.Logger"
        Logger for logging messages.

    Returns
    -------
    dict | None
        File metadata dictionary for the dataset.
    """
    logger.info(f"Scraping files for dataset ID: {dataset_id}")
    response = make_http_request_with_retries(
        client,
        url,
        method=HttpMethod.GET,
        timeout=60,
        delay_before_request=0.1,
    )
    if not response:
        logger.error("Failed to fetch files metadata.")
        return None
    return response.json()


def scrape_files_for_all_datasets(
    client: httpx.Client,
    datasets: list[DatasetMetadata],
    node_base_url: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """Scrape files metadata for all datasets in MDposit API.

    Parameters
    ----------
    client: httpx.Client
        The HTTPX client to use for making requests.
    datasets: list[DatasetMetadata]
        List of datasets to scrape files metadata for.
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
    for dataset_count, dataset in enumerate(datasets, start=1):
        dataset_id = dataset.dataset_id_in_repository
        files_metadata = scrape_files_for_one_dataset(
            client,
            url=f"{node_base_url}/projects/{dataset_id}/filenotes",
            dataset_id=dataset_id,
            logger=logger,
        )
        if not files_metadata:
            continue
        # Extract relevant files metadata.
        logger.info(f"Getting files metadata for dataset: {dataset_id}")
        files_metadata = extract_files_metadata(
            files_metadata, node_base_url, dataset, logger=logger
        )
        all_files_metadata += files_metadata
        # Normalize files metadata with pydantic model (FileMetadata)
        logger.info(f"Total files found: {len(all_files_metadata):,}")
        logger.info(
            "Extracted files metadata for "
            f"{dataset_count:,}/{len(datasets):,} "
            f"({dataset_count / len(datasets):.0%}) datasets."
        )
    return all_files_metadata


def extract_files_metadata(
    raw_metadata: list[dict[str, Any]],
    node_base_url: str,
    dataset: DatasetMetadata,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict[str, Any]]:
    """
    Extract relevant metadata from raw MDposit files metadata.

    Parameters
    ----------
    raw_metadata: dict
        Raw files metadata.
    node_base_url: str
        The unique identifier of the dataset in MDposit.
    dataset: DatasetMetadata
        Normalized dataset to scrape files metadata for.
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
        file_name = Path(mdposit_file.get("filename"))
        node_base_url_for_file = node_base_url.replace("/v1", "")
        file_path_url = (
            f"{node_base_url_for_file}/current/projects/{dataset_id}/files/{file_name}"
        )

        parsed_file = {
            "dataset_repository_name": dataset.dataset_repository_name,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset.dataset_url_in_repository,
            "file_name": str(file_name),
            "file_size_in_bytes": mdposit_file.get("length", None),
            "file_md5": mdposit_file.get("md5", None),
            "file_url_in_repository": file_path_url,
        }
        files_metadata.append(parsed_file)
    logger.info(f"Extracted metadata for {len(files_metadata)} files.")
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

    # Iterate over the nodes
    for data_source_name, base_url in MDDB_REPOSITORIES.items():
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
            node_name=data_source_name,
            logger=logger,
            scraper=scraper,
        )
        if not datasets_raw_metadata:
            logger.critical(f"No datasets found in {data_source_name}.")
            logger.critical("Aborting.")
            sys.exit(1)

        # Select datasets metadata
        datasets_selected_metadata = extract_datasets_metadata(
            datasets_raw_metadata, data_source_name, client, logger=logger
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
            base_url,
            logger=logger,
        )
        # Validate MDDB files metadata with the FileMetadata Pydantic model.
        files_normalized_metadata = normalize_files_metadata(
            files_metadata, logger=logger
        )
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

# Data model for collected data

## datasets.parquet

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

doi:
- type: string
- example: 10.6084/m9.figshare.104603.v3, 10.5281/zenodo.3814193

date_creation
- type: string in the format "YYYY-MM-DD"
- example: 2012-11-25, 2020-07-29
- comment: Date dataset has been created

date_last_modified
- type: string in the format "YYYY-MM-DD"
- example: 2012-11-25, 2020-07-29
- comment: Last date dataset has been modified

date_fetched
- type: string in the format "YYYY-MM-DDTHH:MM:SS"
- example: 2012-11-25T10:34:36
- comment: Date dataset has been fetched

file_number
- type: int
- example: 5, 8

download_number
- type: int
- example: 18, 500
- comment: Total number of downloads for the dataset

view_number
- type: int
- example: 18, 500
- comment: Total number of views for the dataset

license
- type: string
- example: CC-BY-4.0, CC0-1.0

dateset_url
- type: string
- example: https://zenodo.org/record/4537794
- comment: Direct URL to dataset

title
- type: string
- example: "Berger POPE Simulations (versions 1 and 2) 303 K - de Vries repulsive H"

author
- type: string
- example: Hanne Antila, Saara Lautala
- comment: People listed as authors.

keywords
- type: string with keywords separated by ";"
- example: DMTAP;DMPC;cationic lipid bilayer;NaCl;Molecular Dynamics Simulation;Gromacs
- comment: Default is "none".

description:
- type: string
- example: The whole simulation trajectories (28 individual trajectories with 27ns for each) contain the coordinates...
- comment: Description of the dataset as unstructured 'free' text. Length may vary a lot.


## files.parquet

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

file_type
- type: string
- example: mdp, gro, mdp, zip
- comment: Default is "none". File extension do not start with a dot (.gro -> gro)

file_size
- type: float
- example: 2755384, 42550830992
- comment: File size is in bytes.

file_md5
- type: string.
- example: ae4d3b4b88813a52a6fda3e85fa6695f, 989fa719a1a9986b24b4b4dd18dfa8a5
- comment: MD5 checksum

from_zip_file
- type: boolean
- example: True, False
- comment: tells whether or not a given file has been extracted from a zip file

file_name
- type: string
- example: traj118.xtc, mini1.mdp, glucose.itp, coexistence_PSM_POPC.cpt

file_url
- type: string
- example: https://zenodo.org/api/files/79746ec1-ad28-4e6b-acc2-b8febb8b3e76/M-4-400-330.gro
- comment: Direct URL to access the file

origin_zip_file:
- type: string
- example: None, PeptideGraphZenodo.zip, paissoni19diubq.zip
- comment: Default is "none". Name of the zip file the given file has been extracted from.


## gromacs_gro_files.parquet

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

file_name
- type: string
- example: md.gro, prod.gro, S1.gro

atom_number
- type: int
- example: 93700, 18667

has_protein
- type: boolean
- example: True, False
- comment: Whether or not protein residues are present in the system.

has_nucleic
- type: boolean
- example: True, False
- comment: Whether or not nuclein acid bases are present in the system.

has_lipid
- type: boolean
- example: True, False
- comment: Whether or not lipid 'residues' are present in the system.

has_glucid
- type: boolean
- example: True, False
- comment: Whether or not glucid 'residues' are present in the system.

has_water_ion
- type: boolean
- example: True, False
- comment: Whether or not water or ions are present in the system.

## gromacs_mdp_files.parquet

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

file_name
- type: string
- example: md.mdp, prod.mdp, S1.mdp

dt
- type: float
- example: 0.002, 0.001, 0.025
- comment: Time step in ps.

nsteps
- type: int
- examples: 100000000, 500000
- comment: Number of steps performed in the simulation.

temperature
- type: float
- example: 300, 210, 298.15
- comment: Temperature in Klevin.

thermostat
- type: string
- example: Berendsen, V-rescale, no
- comment: Possible thermostat listed here: https://manual.gromacs.org/current/user-guide/mdp-options.html#mdp-tcoupl If no thermostat is used, the value is "no". 

barostat
- type: string
- example: Berendsen, Parrinello-Rahman, no
- comment: Possible thermostat listed here: https://manual.gromacs.org/current/user-guide/mdp-options.html#mdp-pcoupl If no barostat is used, the value is "no". 

integrator
- type: string
- example: md, sd
- commment: Algorithm used to integrate equations of motions.
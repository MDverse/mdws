# Data model for collected data

## datasets.tsv

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


## datasets_text.tsv

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

title
- type: string
- example: "Berger POPE Simulations (versions 1 and 2) 303 K - de Vries repulsive H"

author
- type: string
- example: 
- comment: Not sure we get the uploader name in Zenodo. Maybe get all available authors.

keywords
- type: string with keywords separated by ";"
- example: DMTAP;DMPC;cationic lipid bilayer;NaCl;Molecular Dynamics Simulation;Gromacs
- comment: Default is "none".

description:
- type: string
- example:
- comment: Description of the dataset as unstructured 'free' text. Length may vary a lot.


## files.tsv

dataset_origin
- type: string
- example: zenodo, figshare, osf

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

file_type
- type: string
- example: mdp, gro, mdp, zip
- comment: Default is "none". Remove the dot. For instance, .gro -> gro

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



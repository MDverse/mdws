# Data model for collected data

## datasets.csv

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

origin
- type: string
- example: zenodo, figshare, osf

doi:
- type: string
- example: 10.6084/m9.figshare.104603.v3, 10.5281/zenodo.3814193

date_creation
- type: string in the format YYYY-MM-DD
- example: 2012-11-25, 2020-07-29
- comment: Date dataset has been created

date_last_modified
- type: string in the format YYYY-MM-DD
- example: 2012-11-25, 2020-07-29
- comment: Last date dataset has been modified

date_fetched
- type: string in the format YYYY-MM-DDTHH:MM:SS
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

title
- type: string
- example: "Berger POPE Simulations (versions 1 and 2) 303 K - de Vries repulsive H"

author:
- type: string
- example: 
- comment: Not sure we get the uploader name in Zenodo. Maybe get all available authors.

keywords:
- type: string with keywords separated by ;
- example: DMTAP ; DMPC ; cationic lipid bilayer ; NaCl ; Molecular Dynamics Simulation ; Gromacs


## files.csv

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

origin
- type: string
- example: zenodo, figshare, osf

file_name
- type: string
- example: traj118.xtc, mini1.mdp

file_extension
- type: string
- example: mdp, gro
- comment: remove the dot. For instance, .gro -> gro

file_size
- type: float
- example:
- comment: File size is in KB

file_url
- type: string
- example:

file_md5
- type: string
- example:

file_type
- type: string from a list
- example:
- comment: what is it used for???


- type: string
- example:



- type: string
- example:



- type: string
- example:


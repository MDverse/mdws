# Data model for collected data

## datasets.csv

dataset_id
- type: string
- example: 3814193, M1_gro/5840706

origin
- type: string
- example: zenodo, figshare, osf

title
- type: string
- example: 

date_creation
- type: string
- example: 2012-11-25
- comment: Date dataset has been created

date_last_modified
- type: string
- example: 2012-11-25
- comment: Last date dataset has been modified

date_dataset_fetched
- type: string
- example: 2012-11-25T10:34:36
- comment: Last date dataset has been fetched (delta_time in Python)

author:
- type: string
- example: 
- comment: Not sure we get the uploader name in Zenodo. Maybe get all available authors.

keywords:
- type: string with keywords separated by ;
- example: 

file_number
- type: int
- example: 5, 8

license
- type: string
- example: CC BY, CC BY-SA

download_number
- type: int
- example: 18, 500
- comment: Total number of downloads for the dataset

view_number
- type: int
- example: 18, 500
- comment: Total number of views for the dataset


## files.csv

all the above +

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


#! /usr/bin/env bash

destination="/home/pierre/cloud/gdrive_share/MDbay/outputs_from_mdws/"

rsync -avh results "${destination}"
rsync -avh data/*.tsv "${destination}/data/"
rsync -avh data/*.log "${destination}/data/"

echo "Copied files to ${destination}"

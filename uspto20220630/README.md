This folder contains scripts and documents relevant to the process for updating IUNI1 Postgres and Cadre JanusGraph

- get_pv2022.py : This script will get the granted and pre-granted files from PatentsView and their associated row counts. The information will be placed in a TSV to refer to later. The files will be downloaded, unzipped, and delete the zipped files

- patViews.py : This script will attempt to load and verify table counts to ensure the loading process is accurate and the row counts are as stated on PatentsView

- vp_load.py : This script will load the tables into the database

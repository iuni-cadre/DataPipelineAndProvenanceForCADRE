## Directory Description

This folder contains scripts and documents relevant to the process for updating IUNI1 Postgres and Cadre JanusGraph

- get_pv2022.py : This script will get the granted and pre-granted files from PatentsView and their associated row counts. The information will be placed in a TSV to refer to later. The files will be downloaded, unzipped, and delete the zipped files

- patViews.py : This script will attempt to load and verify table counts to ensure the loading process is accurate and the row counts are as stated on PatentsView

- load_pv.py : This script will load the tables into the database

- get_schema : This script scrapes the schema from the data dictionary page at PatentsView. NOTE: the page is not perfectly up-to-date as of 11/15/2022, be prepared for some errors or cross reference with new release notes/documents

- schema.sql : the schema scraped and updated, no errors should be received when loading new files as of 11/15/2022

- export_nodes_edges.sql : Select statements for all nodes/edges for Cadre

## Diagram:
- new_uspto_diagram.pdf : The new diagram with the updated USPTO data
- new_uspto_diagram.drawio : The editable document for creating the PDF above. Use https://draw.io to edit

### Issues Log:
- Downloads: Following granted tables not showing on data download pages
  - g_gov_interest_contracts
  - g_gov_interest_org
  - g_gov_interest
  - g_cpc_current (downloading using address below still does not work)
  
  Use the same address (as of 11/15/2022: https://s3.amazonaws.com/data.patentsview.org/download/table_name.tsv.zip) like so to download the data

- 
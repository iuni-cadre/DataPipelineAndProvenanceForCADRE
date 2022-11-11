import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re

# create a function for replacing incompatible data types with Postgres
def dat_typ(type):
    ints = [r'(int\(\d+\))',r'(bigint\(\d+\))']

    if type == 'bigint(1)':
        return 'BOOLEAN'
    for i in ints:
        if re.fullmatch(i, type):
            return 'INTEGER'
    if type == 'date':
        return 'VARCHAR(10)'
    elif type == 'double':
        return 'DECIMAL'
    elif type == 'binary(0)':
        return 'VARCHAR(2)'
    else:
        return type.upper()

# urls, will incorporate pre-grant differently?
urls = ['https://patentsview.org/download/data-download-dictionary']
""",'https://patentsview.org/download/pg-data-download-dictionary'"""

pages = [requests.get(u) for u in urls]
soups = [bs(page.content, 'html.parser') for page in pages ]

schema = []

# for each soup (if incorporate pre-grants later)
for soup in soups:
    # obtain the table names
    tab_names = [x.find('td').text for x in soup.find_all('tr',class_='table-head')]

    # initialize json for collecting values of interest
    json ={}
    for name in tab_names:
        # create search name depending on grant/pre-grant
        if name[:2] == 'g_':
            sname = name[2:]
        elif name[:3] == 'pg_':
            sname = name[3:]

        # get the table associated with the current table name, then get all rows, and extract only the values of interest
        table = soup.find('tbody',class_=sname)
        info = table.find_all('tr')[2:-1]
        data = [[name, x.find('td').text, x.find_all('td')[-1].text] for x in info]

        # add dictionary of the data types and using column names as keys for big dict values 
        dic = {x[1]:x[2] for x in data}
        json[name] = dic

    # for each table, create schema
    for key, value in json.items():
        cols = ''
        for k,v in value.items():
            cols = cols + f"{k} {dat_typ(v)},"
        sch = f"CREATE TABLE IF NOT EXISTS patview_core.{key} ({cols[:-1]});"
        schema.append(sch)

# write schema to SQL file
with open('schema.sql','w') as f:
    for s in schema:
        f.write(s+'\n')

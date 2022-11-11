import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import wget
from os import system

# list of urls to extract tables from
url = ['https://patentsview.org/download/data-download-tables','https://patentsview.org/download/pg-download-tables']

# get list of pages from urls
pages = [requests.get(u) for u in url]

# get list of page contents from pages
soups = [bs(page.content, "html.parser") for page in pages]

# initialize data frame
df = pd.DataFrame(columns=['url','row_count'])

# for each url
for soup in soups:

	# get the partition of html with file download url and row counts
	urls = soup.find_all("div",class_='file-title')
	sizes = soup.find_all("td", class_='views-field views-field-field-number-of-rows')

	# get the file download url and the number of row counts from partitions of html
	urls2 = [url.find('a',href=True)['href'] for url in urls]
	sizes2 = [int(size.text.strip().replace(',','')) for size in sizes]

	# create dictionary for entering into dataframe
	dict = {'url':urls2,'row_count':sizes2}

	# create intermediate dataframe
	df2 = pd.DataFrame(dict)

	# add intermediate dataframe to persistent dataframe
	df = pd.concat([df,df2],ignore_index=True)

# create file name column
df['file'] = df['url'].map(lambda x: x.split('/')[-1])
df['file_unzip'] = df['file'].map(lambda x: x[:-4])
df_csv = df.loc[:,['file','file_unzip','row_count']]
df_csv.to_csv('~/to_psql/file_validation.tsv',sep='\t',index=False)

# folder path for downloading
downloads='/N/scratch/shaw10/patentsview/20220630/'

# for each row in dataframe
for index,row in df.iterrows():
	
	# get the path for downloading TSVs and add file name to path for local placement of file
	remote = row['url']
	local = downloads+row['file']

	# download zipped files to scratch
#	wget.download(remote,local)

# unzip all the files
unzip = f'unzip \'{downloads}*.zip\' -d {downloads}'
#system(unzip)

# remove zipped files
rmzip = f'rm {downloads}*.zip'
#system(rmzip)


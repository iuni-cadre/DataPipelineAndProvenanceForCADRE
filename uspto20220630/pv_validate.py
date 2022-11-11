import pandas as pd
from os import listdir

files = listdir('/raw/PatentsView20220630/')

valid = pd.read_csv('~/to_psql/file_validation.tsv',sep='\t',on_bad_lines='warn',low_memory=False)

valid['file'] = valid['file'].map(lambda x: x[:-4])

for f in files:
	path = '/raw/PatentsView20220630/'+f
	print('*'*10,f,'*'*10)
	fcount = valid.loc[valid.file == f, 'row_count'].to_list()[0]
	tab = pd.read_csv(path, sep='\t', low_memory=False,on_bad_lines='skip')
	dcount = tab.shape[0]
	if fcount == dcount:
		print('count of file:', str(fcount),'equals counts of dataframe:',str(dcount))
	else:
		print('-'*20,'FAILED','-'*20)

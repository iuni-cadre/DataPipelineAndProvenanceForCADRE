import pandas as pd
from sqlalchemy import create_engine
from os import listdir

conn = create_engine('postgresql://cadre_admin@localhost:5432/core_data').connect()

path = '/raw/PatentsView20220630/'
files = listdir(path)

for f in files[2:4]:
    tab = f[:-4]

    print('loading data', tab)
    data = pd.read_csv(path+f,sep='\t',low_memory=False, on_bad_lines='error')

    if tab == 'g_application':
        data['rule_47_flag'] = data['rule_47_flag'].map(lambda x: True if x == 1.0 else False)
    
    try:
        print('starting inserts for', tab)
#        data.to_sql(tab, conn, schema='patview_core',index=False, if_exists='append')
	print('inserts complete for', tab)
    except ValueError as e:
        print(e)

import pandas as pd
from sqlalchemy import create_engine
from os import listdir

conn = create_engine('postgresql://cadre_admin@localhost:5432/core_data').connect()

path = '/raw/PatentsView20220630/'
files = listdir(path)

for f in files:
    tab = f[:-4]

    data = pd.read_csv(path+f,sep='\t',low_memory=False, on_bad_lines='error')

    if tab == 'g_application':
        data['file_47_flag'] = data['file_47_flag'].map(lambda x: True if x == 1.0 else False)
    
    #data.to_sql(tab, conn, schema='patview_core',index=False, if_exists='append')
    print(data)
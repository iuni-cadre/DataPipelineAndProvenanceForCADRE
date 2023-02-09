from os import listdir
from os import popen
import pandas as pd

path = '/N/project/iuni_cadre/uspto20220630'

files = listdir(path)

for f in files:
    df = pd.read_csv(f, sep='\t', low_memory=False)
    df_rows = df.shape[0]
    f_rows = int(popen(f'wc -l {path}/{f}').read().split(' ')[0]) - 1
    if f_rows != df_rows:
        print('#'*20, f, 'number of rows do not match','#'*20)
    else:
        print(f, 'good')
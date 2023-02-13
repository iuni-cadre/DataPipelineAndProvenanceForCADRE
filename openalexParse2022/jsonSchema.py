from pyspark.sql.types import *
import sys
import json
from os.path import isfile
from os import system
import pandas as pd

def main():
    """Requires file path argument for sample schema file"""
    try:
        # setting variables
        args = sys.argv
        hdfs_b = '/OpenAlex_202211/'
        schemas = hdfs_b + 'schemas/'

        # test that file for basing schema off is valid
        if isfile(args[1]):

            # open file and converts it to a pandas dataframe
            with open(args[1], 'r') as f:
                j = f.read().split('\n')
            j.remove('')
            data = [json.loads(x) for x in j]
            cols = list(data[0].keys())
            df = pd.DataFrame(data, columns = cols)

            # convert sample file input file to parquet
            new_f = args[2] + '.parquet'
            df.to_parquet(new_f)

            # move sample file to hdfs for spark
            system(f'hdfs dfs -put {new_f} {schemas}{new_f}')
            system(f'rm {new_f}')

        else:
            print('Invalid File Path')
    except Exception as e:
        print(f'Error: {e}')

if __name__ == '__main__':
    main()
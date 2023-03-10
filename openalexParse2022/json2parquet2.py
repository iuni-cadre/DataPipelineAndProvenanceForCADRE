import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

def main():
    """Requires path to OpenAlex entity folder"""
    try:
        # setting variables
        entity = sys.argv[1]
        hdfs_b = '/OpenAlex_202211/'
        schemas = hdfs_b + 'schemas/'
        parquets = hdfs_b + 'parquets/'
        # find and initiate spark
        findspark.init()
        spark= SparkSession.builder \
            .master('yarn') \
            .config('spark.driver.memory','8g') \
            .config('spark.executor.num','49') \
            .config('spark.executor.memory','8g') \
            .config('spark.executor.cores','5') \
            .config('spark.yarn.executor.memoryOverheadFactor','0.2') \
            .config('spark.driver.max.ResultSize','8g') \
            .getOrCreate()

        spark.conf.set('spark.sql.caseSensitive','True')

        # extract schema from sample parquet
        print('Loading schema')
        schema = spark.read.load(f'{schemas}{entity}.parquet')
        print('schema loaded')

        if entity == 'works':
            schema = schema.drop('abstract_inverted_index').schema
            schema.add('abstract_inverted_index', MapType(StringType(), ArrayType(IntegerType())))
        else:
            schema = schema.schema

        # load all jsons using schema from sample parquet
        print('loading JSONs')
        df =spark.read.schema(schema).json(f'{hdfs_b}{entity}/')
        print('JSONs loaded into spark')

        # write all json to parquets
        print('Writing JSONs to Parquets')
        df.coalesce(100).write.mode('overwrite').parquet(f'{parquets}{entity}')
        print('Parquets have been written')
    except Exception as e:
        print(f'Failed JSON to Parquet conversion. Error: \n{e}')

if __name__ == '__main__':
    main()
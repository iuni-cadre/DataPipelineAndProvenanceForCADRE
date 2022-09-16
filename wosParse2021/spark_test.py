import os
import platform
import warnings
import sys

import py4j

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

# All of this initialization rigamarole is taken from the pyspark interactive
# session startup in ${SPARK_HOME}/python/pyspark/python/pyspark/shell.py.
# The initialization creates a Spark context and a Spark-based SQL context.

if os.environ.get("SPARK_EXECUTOR_URI"):
    SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

SparkContext._ensure_initialized()
# This call reads in all the configuration parameters read from the command line to
# initialize a Spark context.
try:
   spark = SparkSession.builder.config('spark.ui.port', '4040').getOrCreate()
   #spark = SparkSession.builder.getOrCreate()
except Exception:
   import sys
   import traceback
   warnings.warn("Failed to initialize Spark session.")
   traceback.print_exc(file=sys.stderr)
   sys.exit(1)

#sc = spark.sparkContext
#sql = spark.sql
#atexit.register(lambda: sc.stop())

#savedStdout = sys.stdout
#with open('/N/u/jmccombs/Carbonate/projects/cadre/spark/out.txt', 'w+') as file:
#   sys.stdout = file
#sys.stdout = savedStdout

# Your workflow begins here
print("PySpark Configuration Properties:")
print(SparkConf().getAll())


 
#sc._jsc.sc().getExecutorMemoryStatus().size()

#data = sc.parallelize(list("Hello World"))
#counts = data.map(lambda x: 
#	(x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
#	 ascending=False).collect()

#for (word, count) in counts:
#    print("{}: {}".format(word, count))

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import array, udf, col, explode, concat_ws, collect_list, sort_array, count, size, coalesce, expr, regexp_replace
spark= SparkSession.builder.config('spark.ui.port','4040').getOrCreate()
spark.conf.set('spark.sql.caseSensitive', 'True') 

WoS = spark.read.format("parquet").load("/WoSraw_2020_all/parquet/part*")


wosEdition = WoS.select(WoS.UID.alias("wosID"),
                        WoS.static_data.summary.EWUID.edition[0]._value.alias('Edition'),
                        WoS.static_data.fullrecord_metadata.category_info.headings.heading[0].alias("heading"),
                    	WoS.static_data.fullrecord_metadata.category_info.subheadings.subheading[0].alias("subheading"),
                    	WoS.static_data.fullrecord_metadata.category_info.subjects.subject["_VALUE"][0].alias("subject")
                       )
                       
             
def clean_text(c):
  c = regexp_replace(c, '"' , '')
  c = regexp_replace(c, '\\\\' , '')
  c = regexp_replace(c, '$s/"//g', '')
  return c
  
 
wosOutput1 = wosEdition.select(
							 col('wosID').alias('wosID'), 
							 col('Edition').alias('Edition'),
							 clean_text(col('heading')).alias('heading'), 
							 clean_text(col('subheading')).alias('subheading'),
							 clean_text(col('subject')).alias('subject')
							)

wosOutput1.coalesce(100).write.option("header","True") \
                               .option("sep","\t") \
                               .option("quoteAll", True) \
                               .mode("overwrite") \
                               .csv('/wosEdition/')         

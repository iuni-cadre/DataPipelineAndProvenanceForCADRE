#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 13:27:19 2021

@author: maahutch
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
#import pandas as pd


spark= SparkSession.builder.config('spark.ui.port','4040').getOrCreate()

authors = spark.read.csv("file:///N/project/mag/mag-2021-01-05/mag/Authors.txt",\
                         header = False,\
                         sep=r'\t')

authors = authors.select(
        authors._c0.cast("int").alias("authorId"),
        authors._c1.cast("int").alias("rank"),
        authors._c2.cast("string").alias("displayName"),
        authors._c3.cast("string").alias("normalizedName"),
        authors._c4.cast("int").alias("lastKnownAffiliationId"),
        authors._c5.cast("int").alias("paperCount"),
        authors._c7.cast("int").alias("citationCount"), 
        to_timestamp(authors._c8, "yyyy-MM-dd").alias("date")
        )

#authors.printSchema()


#authors.toPandas().to_csv('/N/project/mag/authors_II.csv', \
#                          sep = '~',\
#                          head = True, \
#                         index = False)
#.mode(SaveMode.Overwrite)
                     
authors.coalesce(10).write.csv('/N/project/mag/authors_II')
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import array, udf, col, explode, concat_ws, collect_list, sort_array, count, size, coalesce, expr, regexp_replace, array_contains
spark= SparkSession.builder.config('spark.ui.port','4040').getOrCreate()

spark.conf.set('spark.sql.caseSensitive', 'True') 

WoS3 = spark.read.format("parquet").load("/WoSraw_2020_all/parquet/part*")

WoS = WoS3.filter(WoS3.static_data.summary.pub_info._pubyear.contains('2020'))

wos = WoS.select(WoS.UID.alias("wosID"),
                 WoS.static_data.summary.pub_info._pubyear.alias("Year"),
                 WoS.static_data.summary.pub_info._pubtype.alias("publicationType"),
                 WoS.static_data.summary.doctypes.doctype[0].alias("documentType"),
                 WoS.static_data.summary.publishers.publisher.names["name"]["full_name"].alias("publisher"),
                 WoS.static_data.summary.publishers.publisher.names["name"].full_name.alias("publicationName"),
                 WoS.static_data.summary.pub_info.page._page_count.alias("pageCount"),
                 concat_ws(', ', WoS.static_data.fullrecord_metadata.fund_ack.fund_text.p).alias("fundingText"), 
                 concat_ws(', ', WoS.static_data.fullrecord_metadata.addresses.address_name.names["name"][0].full_name).alias("reprintAuthor"),
                 WoS.static_data.fullrecord_metadata.reprint_addresses.address_name.address_spec.full_address[0].alias("reprintAuthorAddress"),
                 WoS.static_data.fullrecord_metadata.addresses.address_name.address_spec['full_address'].alias('full_address')
                            )  
                            
wos = wos.withColumn("full_address", concat_ws(", ",  col("full_address")))

###################################################################                           
wos_test =  WoS.select(WoS.UID.alias("wosID"),
                       WoS.static_data.fullrecord_metadata.addresses.address_name.address_spec.organizations.organization[1].alias("org"))

wos_test2 = wos_test.withColumn("org1", wos_test["org"].getItem(0))
                            
wos_test2 = wos_test2.withColumn("org2", wos_test["org"].getItem(1))

wos_test2 = wos_test2.withColumn("org3", wos_test["org"].getItem(2))

wos_test2 = wos_test2.withColumn("org4", wos_test["org"].getItem(3))

wosEorg = wos_test2.withColumn("org1", explode(array("org1"))).select("*", col("org1")["_VALUE"].alias("Eorg1"))                       
wosEorg = wosEorg.withColumn("org2", explode(array("org2"))).select("*", col("org2")["_VALUE"].alias("Eorg2"))
wosEorg = wosEorg.withColumn("org3", explode(array("org3"))).select("*", col("org3")["_VALUE"].alias("Eorg3"))                                                                           
wosEorg = wosEorg.withColumn("org4", explode(array("org4"))).select("*", col("org4")["_VALUE"].alias("Eorg4"))                           



wosEorg = wosEorg.select('wosID', 'Eorg1', 'Eorg2', 'Eorg3', 'Eorg4')


#wosEorg = wosEorg.filter(col("Eorg").like('Indiana%'))

#wosEorg.show(50, truncate = True)

###################################################################
#BUILD ISSN/EISSN/DOI TABLE

wosIdPlus = WoS.select(WoS.UID.alias("wosID"),
                       WoS.dynamic_data.cluster_related.identifiers.identifier.alias('ids'))

wosIdPlus2 = wosIdPlus.withColumn("ids", explode("ids")).select("*", col("ids")["_type"].alias("altID"))

wosIdDf = wosIdPlus.select(wosIdPlus.wosID.alias("wosID"))

wosIdPlus3 = wosIdPlus2.select('wosID', 'ids.*')



issn   = wosIdPlus3.filter(wosIdPlus3._type == "issn")
issn2  = issn.groupBy("wosID").agg(sort_array(collect_list("_value")).alias("_value"))
issn3  = issn2.withColumn("_value", concat_ws("| ", issn2._value))
issn4  = issn3.select(col('wosID').alias("wosID"), col('_value').alias('issn'))

doi   = wosIdPlus3.filter(wosIdPlus3._type == "doi")
doi2  = doi.groupBy("wosID").agg(sort_array(collect_list("_value")).alias("_value"))
doi3  = doi2.withColumn("_value", concat_ws("| ", doi2._value))
doi4   = doi3.select(col('wosID').alias('wosID'), col('_value').alias('doi'))

eissn   = wosIdPlus3.filter(wosIdPlus3._type == "eissn")
eissn2  = eissn.groupBy("wosID").agg(sort_array(collect_list("_value")).alias("_value"))
eissn3  = eissn2.withColumn("_value", concat_ws("| ", eissn2._value))
eissn4  = eissn3.select(col('wosID').alias('wosID'), col('_value').alias('eissn'))

isbn   = wosIdPlus3.filter(wosIdPlus3._type == "isbn")
isbn2  = isbn.groupBy("wosID").agg(sort_array(collect_list("_value")).alias("_value"))
isbn3  = isbn2.withColumn("_value", concat_ws("| ", isbn2._value))
isbn4  = isbn3.select(col('wosID').alias('wosID'), col('_value').alias('isbn'))    

pmid   = wosIdPlus3.filter(wosIdPlus3._type == "pmid")
pmid2  = pmid.groupBy("wosID").agg(sort_array(collect_list("_value")).alias("_value"))
pmid4  = pmid2.select(col('wosID').alias('wosID'), concat_ws("| ", pmid2._value).alias('pmid'))

wosIdPlus4 = wosIdDf.join(doi4,   wosIdDf['wosID'] == doi4['wosID'],   how='left') \
                    .join(eissn4, wosIdDf['wosID'] == eissn4['wosID'], how='left') \
                    .join(isbn4,  wosIdDf['wosID'] == isbn4['wosID'],  how='left') \
                    .join(pmid4,  wosIdDf['wosID'] == pmid4['wosID'],  how='left') \
                    .join(issn4,  wosIdDf['wosID'] == issn4['wosID'],  how='left') \
                 .select(coalesce(wosIdDf.wosID, issn4.wosID, doi4.wosID, eissn4.wosID, isbn4.wosID, pmid4.wosID).alias('wosID'), 
                        issn4.issn, doi4.doi, eissn4.eissn, isbn4.isbn, pmid4.pmid)

####################################################################################################
#BUILD AUTHOR ARRAY
wos5 = WoS.select(WoS.UID.alias("wosID"), WoS.static_data.summary.names['name'].alias("author"))

wos6 = wos5.withColumn("author", explode("author")).select("*", col("author")["_seq_no"].alias("seq_no"),
                                                                col("author")["display_name"].alias("standardNames")
                                                          )

wos_author = wos6.select("wosID", concat_ws(" ", wos6.standardNames, wos6.seq_no).alias("standardNames")
                        )


wos_auth2 = wos_author.groupBy("wosID").agg(sort_array(collect_list("standardNames")).alias("standardNames")
                                           )  
wos_auth2 = wos_auth2.select(
   col('wosID'),
   concat_ws("| ", col('standardNames')).alias('standardNames'),  
   size('standardNames').alias('count')
)

wos_auth2 = wos_auth2.drop('count')          
                        
################################################################################################

                
wtOutput = wos.join(wosIdPlus4, wos['wosID'] == wosIdPlus4['wosID'], how='full') \
              .join(wosEorg,    wos['wosID'] == wosEorg['wosID'],    how='full') \
              .join(wos_auth2,  wos['wosID'] == wos_auth2['wosID'],  how='full') \
            .select(coalesce(wos.wosID,
                             wosIdPlus4.wosID,
                             wosEorg.wosID,
                             wos_auth2.wosID).alias('wosId'),
                   wos.Year,
                   wos.publicationType, 
                   wos.documentType, 
                   wos.publisher, 
                   wos.publicationName, 
                   wos.pageCount, 
                   wos.fundingText, 
                   wos.reprintAuthor, 
                   wos.reprintAuthorAddress,
                   wos.full_address,
                    wos_auth2.standardNames,
                       wosIdPlus4.issn, 
                       wosIdPlus4.doi, 
                       wosIdPlus4.eissn, 
                       wosIdPlus4.isbn,
                       wosIdPlus4.pmid,
                        wosEorg.Eorg1,
                        wosEorg.Eorg2,
                        wosEorg.Eorg3,
                        wosEorg.Eorg4)
                        
#################################################                      
#wtOutput2 = wtOutput.filter(wtOutput.Eorg.like('Indiana University System'))

wtOutput2 = wtOutput.filter(col("Eorg3").like('Indiana University Bloomington'))

#################################################

wtOutput2.coalesce(100).write.option("header","True") \
                               .option("sep","\t") \
                               .option("quoteAll", True) \
                               .mode("overwrite") \
                               .csv('/wtQuery')



from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, concat_ws, collect_list, sort_array, count, size, coalesce, expr
spark= SparkSession.builder.config('spark.ui.port','4040').getOrCreate()

spark.conf.set('spark.sql.caseSensitive', 'True') 

WoS = spark.read.format("parquet").load("/WoSraw_2020_all/parquet/part-00826-5476e87f-146e-4916-913b-522638c0d728-c000.snappy.parquet")

wos4a = WoS.select(WoS.UID.alias("wosID"),
				  WoS.dynamic_data.ic_related.oases['_is_OA'].alias('isOpenAccess'),
                  concat_ws(", ", WoS.dynamic_data.ic_related.oases.oas['_type']).alias('openAccessType'),	
                  WoS.static_data.fullrecord_metadata.abstracts.abstract.abstract_text.p[0].alias("abstract"),
                  WoS.static_data.fullrecord_metadata.fund_ack.fund_text.p.alias("fundingText"),
                  WoS.static_data.fullrecord_metadata.references._count.alias("citedReferenceCount"),
                  WoS.static_data.fullrecord_metadata.addresses.address_name.address_spec['full_address'].alias('full_address'),
                  WoS.static_data.fullrecord_metadata.reprint_addresses.address_name.address_spec.full_address[0].alias('reprintAddress'),
                  WoS.static_data.fullrecord_metadata.references.reference.art_no.alias('articleNumber'),
                  WoS.static_data.summary.pub_info._pubyear.alias("publicationYear"),
                  WoS.static_data.summary.pub_info._coverdate.alias("publicationDate"),
                  WoS.static_data.summary.pub_info._vol.alias("volume"),
                  WoS.static_data.summary.pub_info._issue.alias("issue"),
                  WoS.static_data.summary.pub_info._part_no.alias("partNumber"),
                  WoS.static_data.summary.pub_info._supplement.alias("supplement"),
                  WoS.static_data.summary.pub_info._special_issue.alias("specialIssue"),
                  WoS.static_data.summary.pub_info._early_access_date.alias("earlyAccessDate"),
                  WoS.static_data.summary.pub_info.page._begin.alias("startPage"),
                  WoS.static_data.summary.pub_info.page._end.alias("endPage"),
                  WoS.static_data.summary.pub_info.page._page_count.alias("numberOfPages"),
                  WoS.static_data.summary.publishers.publisher.address_spec.city.alias("publisherCity"),
                  WoS.static_data.summary.publishers.publisher.address_spec.full_address.alias("publisherAddress"),
                  WoS.static_data.summary.publishers.publisher.names["name"]["full_name"].alias('publisher'),
                  WoS.static_data.item.keywords_plus.keyword.alias('keywordsPlus'),
                  WoS.static_data.summary.conferences.conference.conf_dates.conf_date._VALUE.alias("conferenceDate"),
                  WoS.static_data.summary.conferences.conference.sponsors.sponsor[0].alias("conferenceSponsor"),
                  WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_host.alias("conferenceHost"),
                  WoS.static_data.summary.conferences.conference.conf_titles.conf_title[0].alias("conferenceTitle"),
                  WoS.static_data.summary.doctypes.doctype[0].alias('documentType')
                 )
                 
wos4 = wos4a.withColumn("fundingText",        concat_ws(", ",  col("fundingText")))       \
            .withColumn("full_address",       concat_ws(", ",  col("full_address")))      \
            .withColumn("articleNumber",      concat_ws(", ",  col("articleNumber")))     \
            .withColumn("keywordsPlus",       concat_ws(", ",  col("keywordsPlus")))      \
            .withColumn("conferenceDate",     concat_ws(", ",  col("conferenceDate")))    \
            .withColumn("conferenceSponsor",  concat_ws(", ",  col("conferenceSponsor"))) \
            .withColumn("conferenceHost",     concat_ws(", ",  col("conferenceHost"))) 
            
            
            
#BUILD AUTHOR ARRAY
wos5 = WoS.select(WoS.UID.alias("wosID"), WoS.static_data.contributors.contributor.alias("author"))

wos6 = wos5.withColumn("author", explode("author")).select("*", col("author")["name"]["_seq_no"].alias("seq_no"),
                                                                col("author")["name"]["full_name"].alias("fullname"),
                                                                col("author")["name"]["_r_id"].alias("RIDs"), 
                                                                col("author")["name"]["_orcid_id"].alias("ORCID"),
                                                                col("author")["name"]["display_name"].alias("standardNames")    
                                                          )

wos_author = wos6.select("wosID", concat_ws(" ", wos6.fullname,      wos6.seq_no).alias("author"), 
                                  concat_ws(" ", wos6.RIDs,          wos6.seq_no).alias("RIDs"), 
                                  concat_ws(" ", wos6.ORCID,         wos6.seq_no).alias("ORCID"),
                                  concat_ws(" ", wos6.standardNames, wos6.seq_no).alias("standardNames")
                        )


wos_auth2 = wos_author.groupBy("wosID").agg(sort_array(collect_list("author")).alias("authors"),
                                            sort_array(collect_list("RIDs")).alias("RIDs"),
                                            sort_array(collect_list("ORCID")).alias("ORCID"),
                                            sort_array(collect_list("standardNames")).alias("standardNames")
                                           )  
                                            

wos_auth2 = wos_auth2.select(
   col('wosID'),
   concat_ws("| ", col('RIDs')).alias('RIDs'),
   concat_ws("| ", col('ORCID')).alias('ORCID'),
   concat_ws("| ", col('standardNames')).alias('standardNames'),  
   concat_ws("| ",col('authors')).alias('authors'), 
   size('authors').alias('count')
)

wos_auth2 = wos_auth2.drop('count')     


#EMAIL ADDRESSES
wosEmail = WoS.select(WoS.UID.alias("wosID"), WoS.static_data.fullrecord_metadata.addresses.address_name.names.alias("addr"))

wosEmail2 = wosEmail.withColumn("addr", explode("addr")).select("*",
                                                                col("addr")["name"]["_seq_no"].alias("seq_no"),
                                                                col("addr")["name"]["email_addr"].alias("emailAddress"),
                                                          )

wosEmail3 = wosEmail2.select("wosID",
                                  concat_ws(" ", wosEmail2.emailAddress,  wosEmail2.seq_no).alias("emailAddress")
                        )


wosEmail4 = wosEmail3.groupBy("wosID").agg(
                                            sort_array(collect_list("emailAddress")).alias("emailAddress")
                                           )  
                                            

wosEmail5 = wosEmail4.select(
   col('wosID'),
   concat_ws("| ", col('emailAddress')).alias('emailAddress'), 
   size('emailAddress').alias('count')
)

wosEmail5 = wosEmail5.drop('count')
     
            
            
#BUILD TITLE COLUMNS

wos_title = WoS.select(WoS.UID.alias("wosID"),
                      WoS.static_data.summary.titles.title.alias('title'))

wosTitle2 = wos_title.withColumn("title", explode("title")).select("*", col("title")["_type"].alias("paperTitle"))


wosTitle3 = wosTitle2.select('wosID', 'title.*')
                                       
item   = wosTitle3.filter(wosTitle3._type == "item")
item   = item.select('wosID', col('_VALUE').alias('paperTitle'))

source = wosTitle3.filter(wosTitle3._type == "source")
source = source.select(col('wosID').alias('id2'), col('_VALUE').alias('journalTitle'))

journalAbbr = wosTitle3.filter(wosTitle3._type == "source_abbrev")
journalAbbr = journalAbbr.select(col('wosID').alias('id3'), col('_VALUE').alias('journalAbbrev'))

journalISO = wosTitle3.filter(wosTitle3._type == "abbrev_iso")
journalISO = journalISO.select(col('wosID').alias('id4'), col('_VALUE').alias('journalISO'))

wosTitle4 = item.join(source,      item['wosID'] == source['id2'], how='full') \
                .join(journalAbbr, item['wosID'] == journalAbbr['id3'], how='full') \
                .join(journalISO,  item['wosID'] == journalISO['id4'], how='full') \
                .select(coalesce(item.wosID, source.id2, journalAbbr.id3, journalISO.id4).alias('wosID'), 
                        item.paperTitle, 
                        source.journalTitle,
                        journalAbbr.journalAbbrev, 
                        journalISO.journalISO)
                      
                      
#BUILD ISSN/EISSN/DOI TABLE

wosIdPlus = WoS.select(WoS.UID.alias("wosID"),
                       WoS.dynamic_data.cluster_related.identifiers.identifier.alias('ids'))

wosIdPlus2 = wosIdPlus.withColumn("ids", explode("ids")).select("*", col("ids")["_type"].alias("altID"))

wosIdPlus3 = wosIdPlus2.select('wosID', 'ids.*')

issn   = wosIdPlus3.filter(wosIdPlus3._type == "issn")
issn   = issn.select('wosID', col('_value').alias('issn'))

doi   = wosIdPlus3.filter(wosIdPlus3._type == "doi")
doi   = doi.select(col('wosID').alias('id2'), col('_value').alias('doi'))

eissn   = wosIdPlus3.filter(wosIdPlus3._type == "eissn")
eissn   = eissn.select(col('wosID').alias('id3'), col('_value').alias('eissn'))

isbn   = wosIdPlus3.filter(wosIdPlus3._type == "isbn")
isbn   = isbn.select(col('wosID').alias('id4'), col('_value').alias('isbn'))

pmid   = wosIdPlus3.filter(wosIdPlus3._type == "pmid")
pmid   = pmid.select(col('wosID').alias('id5'), col('_value').alias('pmid'))

wosIdPlus4 = issn.join(doi,   issn['wosID'] == doi['id2'],  how='full') \
                 .join(eissn, issn['wosID'] == eissn['id3'],how='full') \
                 .join(isbn,  issn['wosID'] == isbn['id4'], how='full') \
                 .join(pmid,  issn['wosID'] == pmid['id5'], how='full') \
                 .select(coalesce(issn.wosID, doi.id2, eissn.id3, isbn.id4, pmid.id5).alias('wosID'), 
                         issn.issn, doi.doi, eissn.eissn, isbn.isbn, pmid.pmid)


#BUILD CONFERENCE LOCATION

wosCoLo = WoS.select(WoS.UID.alias("wosID"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_city.alias("conf_city"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_host.alias("conf_host"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_state.alias("conf_state")
                    )

wosCoLo2 = wosCoLo.withColumn('conferenceLocation', concat_ws(", ", wosCoLo.conf_city, wosCoLo.conf_host, wosCoLo.conf_state))


wosCoLo2 = wosCoLo2.drop('conf_city')
wosCoLo2 = wosCoLo2.drop('conf_host')
wosCoLo2 = wosCoLo2.drop('conf_state')



#BUILD FUNDING ORG String

wosFo = WoS.select(WoS.UID.alias("wosID"),
                WoS.static_data.fullrecord_metadata.fund_ack.grants.grant.grant_agency[0].alias('fundingOrgs'))


wosFo2 = wosFo.withColumn("fundingOrgs", col("fundingOrgs").getField("_VALUE"))

wosFo3 = wosFo2.withColumn("fundingOrgs", concat_ws("| ", wosFo2.fundingOrgs))


wosOutput = wos4.join(wos_auth2,  wos4['wosID'] == wos_auth2['wosID'], how='full') \
                .join(wosEmail5,  wos4['wosID'] == wosEmail5['wosID'], how='full') \
                .join(wosTitle4,  wos4['wosID'] == wosTitle4['wosID'], how='full') \
                .join(wosIdPlus4, wos4['wosID'] == wosIdPlus4['wosID'],how='full') \
                .join(wosCoLo2,   wos4['wosID'] == wosCoLo2['wosID'],  how='full') \
                .join(wosFo3,     wos4['wosID'] == wosFo3['wosID'],    how='full') \
                .select(coalesce(wos4.wosID, 
                                 wos_auth2.wosID, 
                                 wosEmail5.wosID, 
                                 wosTitle4.wosID, 
                                 wosIdPlus4.wosID,
                                 wosCoLo2.wosID,
                                 wosFo3.wosID).alias('wosID'),
                  wos4.abstract,
                  wos4.fundingText,        
                  wos4.citedReferenceCount,      
                  wos4.full_address,     
                  wos4.reprintAddress,   
                  wos4.articleNumber,
                  wos4.publicationYear,
                  wos4.publicationDate,
                  wos4.volume,
                  wos4.issue,
                  wos4.partNumber,
                  wos4.supplement,
                  wos4.specialIssue,
                  wos4.earlyAccessDate,
                  wos4.startPage,
                  wos4.endPage,
                  wos4.numberOfPages,
                  wos4.publisherCity,
                  wos4.publisherAddress,
                  wos4.publisher,
                  wos4.keywordsPlus,
                  wos4.conferenceDate,
                  wos4.conferenceSponsor,
                  wos4.conferenceHost,
                  wos4.conferenceTitle,
                  wos4.documentType,
                      wos_auth2.RIDs,
                      wos_auth2.ORCID,
                      wos_auth2.standardNames,
                      wos_auth2.authors,
                        wosEmail5.emailAddress,
                            wosTitle4.paperTitle, 
                            wosTitle4.journalTitle,
                            wosTitle4.journalAbbrev, 
                            wosTitle4.journalISO,
                                wosIdPlus4.issn,
                                wosIdPlus4.doi,
                                wosIdPlus4.eissn, 
                                wosIdPlus4.isbn,
                                wosIdPlus4.pmid,
                                    wosCoLo2.conferenceLocation,
                                        wosFo3.fundingOrgs
                       )
                        
wosOutput.coalesce(64).write.option("header","True") \
                               .option("sep","\t") \
                               .option("quoteAll", False) \
                               .option("emptyValue", None) \
                               .option("nullValue", None)\
                               .mode("overwrite") \
                               .csv('/WoSraw_2020/nodes')                       
                        




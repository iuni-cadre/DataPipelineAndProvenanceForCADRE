from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, concat_ws, collect_list, sort_array, count, size, coalesce, expr, regexp_replace
spark= SparkSession.builder.config('spark.ui.port','4040').getOrCreate()

spark.conf.set('spark.sql.caseSensitive', 'True') 

WoS = spark.read.format("parquet").load("/WoSraw_2020_all/parquet/part*.snappy.parquet")

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
pmid3  = pmid2.withColumn("_value", concat_ws("| ", pmid2._value))
pmid4  = pmid3.select(col('wosID').alias('wosID'), col('_value').alias('pmid'))


wosIdPlus4 = wosIdDf.join(doi4,   wosIdDf['wosID'] == doi4['wosID'],   how='left') \
                    .join(eissn4, wosIdDf['wosID'] == eissn4['wosID'], how='left') \
                    .join(isbn4,  wosIdDf['wosID'] == isbn4['wosID'],  how='left') \
                    .join(pmid4,  wosIdDf['wosID'] == pmid4['wosID'],  how='left') \
                    .join(issn4,  wosIdDf['wosID'] == issn4['wosID'],  how='left') \
                 .select(coalesce(wosIdDf.wosID, issn4.wosID, doi4.wosID, eissn4.wosID, isbn4.wosID, pmid4.wosID).alias('wosID'), 
                         issn4.issn, doi4.doi, eissn4.eissn, isbn4.isbn, pmid4.pmid)



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
                  wos4.isOpenAccess,
                  wos4.openAccessType,
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
                       
def clean_text(c):
  c = regexp_replace(c, '"' , '')
  c = regexp_replace(c, '\\\\' , '')
  c = regexp_replace(c, '$s/"//g', '')
  return c
  
  
#CLEAN TEXT
wosOutput1 = wosOutput.select(
						clean_text(col('wosID')).alias('wosID'),
                        clean_text(col("isOpenAccess")).alias("isOpenAccess"), 
                        clean_text(col("openAccessType")).alias("openAccessType"),
                        clean_text(col("abstract")).alias('abstract'),
                        clean_text(col("fundingText")).alias('fundingText'),        
                        clean_text(col("citedReferenceCount")).alias('citedReferenceCount'),      
                        clean_text(col("full_address")).alias('full_address'),     
                        clean_text(col("reprintAddress")).alias('reprintAddress'),   
                        clean_text(col("articleNumber")).alias('articleNumber'),
                        clean_text(col("publicationYear")).alias('publicationYear'),
                        clean_text(col("publicationDate")).alias('publicationDate'),
                        clean_text(col("volume")).alias('volume'),
                        clean_text(col("issue")).alias('issue'),
                        clean_text(col("partNumber")).alias('partNumber'),
                        clean_text(col("supplement")).alias('supplement'),
                        clean_text(col("specialIssue")).alias('specialIssue'),
                        clean_text(col("earlyAccessDate")).alias('earlyAccessDate'),
                        clean_text(col("startPage")).alias('startPage'),
                        clean_text(col("endPage")).alias('endPage'),
                        clean_text(col("numberOfPages")).alias('numberOfPages'),
                        clean_text(col("publisherCity")).alias('publisherCity'),
                        clean_text(col("publisherAddress")).alias('publisherAddress'),
                        clean_text(col("publisher")).alias('publisher'),
                        clean_text(col("keywordsPlus")).alias('keywordsPlus'),
                        clean_text(col("conferenceDate")).alias('conferenceDate'),
                        clean_text(col("conferenceSponsor")).alias('conferenceSponsor'),
                        clean_text(col("conferenceHost")).alias('conferenceHost'),
                        clean_text(col("conferenceTitle")).alias('conferenceTitle'),
                        clean_text(col("documentType")).alias('documentType'),
                        clean_text(col("RIDs")).alias('RIDs'),
                        clean_text(col("ORCID")).alias('ORCID'),
                        clean_text(col("standardNames")).alias('standardNames'),
                        clean_text(col("authors")).alias('authors'),
                        clean_text(col("emailAddress")).alias('emailAddress'),
                        clean_text(col("paperTitle")).alias('paperTitle'), 
                        clean_text(col("journalTitle")).alias('journalTitle'),
                        clean_text(col("journalAbbrev")).alias('journalAbbrev'), 
                        clean_text(col("journalISO")).alias('journalISO'),
                        clean_text(col("issn")).alias('issn'),
                        clean_text(col("doi")).alias('doi'),
                        clean_text(col("eissn")).alias('eissn'), 
                        clean_text(col("isbn")).alias('isbn'),
                        clean_text(col("pmid")).alias('pmid'),
                        clean_text(col("conferenceLocation")).alias('conferenceLocation'),
                        clean_text(col("fundingOrgs")).alias('fundingOrgs')
)  
                        
wosOutput1.coalesce(100).write.option("header","True") \
                               .option("sep","\t") \
                               .option("quoteAll", True) \
                               .mode("overwrite") \
                               .csv('/WoSraw_2020_all/nodes2')                       
                        




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



#FIRST JOIN

wosOutput1 = wos4.join(wos_auth2,  wos4['wosID'] == wos_auth2['wosID'], how='full') \
                 .select(coalesce(wos4.wosID, wos_auth2.wosID).alias('wosID'),
                  wos4.isOpenAccess.alias('isOpenAccess'),
                  wos4.openAccessType.alias('openAccessType'), 
                  wos4.abstract.alias('abstract'),
                  wos4.fundingText.alias('fundingText'),        
                  wos4.citedReferenceCount.alias('citedReferenceCount'),      
                  wos4.full_address.alias('full_address'),     
                  wos4.reprintAddress.alias('reprintAddress'),   
                  wos4.articleNumber.alias('articleNumber'),
                  wos4.publicationYear.alias('publicationYear'),
                  wos4.publicationDate.alias('publicationDate'),
                  wos4.volume.alias('volume'),
                  wos4.issue.alias('issue'),
                  wos4.partNumber.alias('partNumber'),
                  wos4.supplement.alias('supplement'),
                  wos4.specialIssue.alias('specialIssue'),
                  wos4.earlyAccessDate.alias('earlyAccessDate'),
                  wos4.startPage.alias('startPage'),
                  wos4.endPage.alias('endPage'),
                  wos4.numberOfPages.alias('numberOfPages'),
                  wos4.publisherCity.alias('publisherCity'),
                  wos4.publisherAddress.alias('publisherAddress'),
                  wos4.publisher.alias('publisher'),
                  wos4.keywordsPlus.alias('keywordsPlus'),
                  wos4.conferenceDate.alias('conferenceDate'),
                  wos4.conferenceSponsor.alias('conferenceSponsor'),
                  wos4.conferenceHost.alias('conferenceHost'),
                  wos4.conferenceTitle.alias('conferenceTitle'),
                  wos4.documentType.alias('documentType'),
                      wos_auth2.RIDs.alias('RIDs'),
                      wos_auth2.ORCID.alias('ORCID'),
                      wos_auth2.standardNames.alias('standardNames'),
                      wos_auth2.authors.alias('authors')
                      )
                                     

#SECOND JOIN 

wosOutput2 = wosOutput1.join(wosEmail5,  wosOutput1['wosID'] == wosEmail5['wosID'], how='full') \
                       .select(coalesce(wosOutput1.wosID, wosEmail5.wosID).alias('wosID'),
                        wosOutput1.isOpenAccess.alias('isOpenAccess'),
                        wosOutput1.openAccessType.alias('openAccessType'), 
                        wosOutput1.abstract.alias('abstract'),
                  		wosOutput1.fundingText.alias('fundingText'),        
                  		wosOutput1.citedReferenceCount.alias('citedReferenceCount'),      
                  		wosOutput1.full_address.alias('full_address'),     
                  		wosOutput1.reprintAddress.alias('reprintAddress'),   
                  		wosOutput1.articleNumber.alias('articleNumber'),
                  		wosOutput1.publicationYear.alias('publicationYear'),
                  		wosOutput1.publicationDate.alias('publicationDate'),
                  		wosOutput1.volume.alias('volume'),
                  		wosOutput1.issue.alias('issue'),
                  		wosOutput1.partNumber.alias('partNumber'),
                  		wosOutput1.supplement.alias('supplement'),
                  		wosOutput1.specialIssue.alias('specialIssue'),
                  		wosOutput1.earlyAccessDate.alias('earlyAccessDate'),
                  		wosOutput1.startPage.alias('startPage'),
                  		wosOutput1.endPage.alias('endPage'),
                  		wosOutput1.numberOfPages.alias('numberOfPages'),
                  		wosOutput1.publisherCity.alias('publisherCity'),
                  		wosOutput1.publisherAddress.alias('publisherAddress'),
                  		wosOutput1.publisher.alias('publisher'),
                  		wosOutput1.keywordsPlus.alias('keywordsPlus'),
	                    wosOutput1.conferenceDate.alias('conferenceDate'),
       		            wosOutput1.conferenceSponsor.alias('conferenceSponsor'),
             		    wosOutput1.conferenceHost.alias('conferenceHost'),
                  		wosOutput1.conferenceTitle.alias('conferenceTitle'),
	                    wosOutput1.documentType.alias('documentType'),
      	                wosOutput1.RIDs.alias('RIDs'),
      	                wosOutput1.ORCID.alias('ORCID'),
      	                wosOutput1.standardNames.alias('standardNames'),
      	                wosOutput1.authors.alias('authors'),
                  			wosEmail5.emailAddress.alias('emailAddress')
                  		)    
                  		
 #THIRD JOIN

wosOutput3 = wosOutput2.join(wosTitle4,  wosOutput2['wosID'] == wosTitle4['wosID'], how='full')\
                       .select(coalesce(wosOutput2.wosID, wosTitle4.wosID).alias('wosID'),
                        wosOutput2.isOpenAccess.alias('isOpenAccess'),
                        wosOutput2.openAccessType.alias('openAccessType'), 
                        wosOutput2.abstract.alias('abstract'),
                  		wosOutput2.fundingText.alias('fundingText'),        
                  		wosOutput2.citedReferenceCount.alias('citedReferenceCount'),      
                  		wosOutput2.full_address.alias('full_address'),     
                  		wosOutput2.reprintAddress.alias('reprintAddress'),   
                  		wosOutput2.articleNumber.alias('articleNumber'),
                  		wosOutput2.publicationYear.alias('publicationYear'),
                  		wosOutput2.publicationDate.alias('publicationDate'),
                  		wosOutput2.volume.alias('volume'),
                  		wosOutput2.issue.alias('issue'),
                  		wosOutput2.partNumber.alias('partNumber'),
                  		wosOutput2.supplement.alias('supplement'),
                  		wosOutput2.specialIssue.alias('specialIssue'),
                  		wosOutput2.earlyAccessDate.alias('earlyAccessDate'),
                  		wosOutput2.startPage.alias('startPage'),
                  		wosOutput2.endPage.alias('endPage'),
                  		wosOutput2.numberOfPages.alias('numberOfPages'),
                  		wosOutput2.publisherCity.alias('publisherCity'),
                  		wosOutput2.publisherAddress.alias('publisherAddress'),
                  		wosOutput2.publisher.alias('publisher'),
                  		wosOutput2.keywordsPlus.alias('keywordsPlus'),
	                    wosOutput2.conferenceDate.alias('conferenceDate'),
       		            wosOutput2.conferenceSponsor.alias('conferenceSponsor'),
             		    wosOutput2.conferenceHost.alias('conferenceHost'),
                  		wosOutput2.conferenceTitle.alias('conferenceTitle'),
	                    wosOutput2.documentType.alias('documentType'),
      	                wosOutput2.RIDs.alias('RIDs'),
      	                wosOutput2.ORCID.alias('ORCID'),
      	                wosOutput2.standardNames.alias('standardNames'),
      	                wosOutput2.authors.alias('authors'),
                  		wosOutput2.emailAddress.alias('emailAddress'),
                  			wosTitle4.paperTitle.alias('paperTitle'), 
                            wosTitle4.journalTitle.alias('journalTitle'),
                            wosTitle4.journalAbbrev.alias('journalAbbrev'), 
                            wosTitle4.journalISO.alias('journalISO')
                  		) 

#FOURTH JOIN  

wosOutput4 = wosOutput3.join(wosIdPlus4,  wosOutput3['wosID'] == wosIdPlus4['wosID'], how='full') \
         				.select(coalesce(wosOutput3.wosID, wosIdPlus4.wosID).alias('wosID'), 
         				wosOutput3.isOpenAccess.alias('isOpenAccess'),
                        wosOutput3.openAccessType.alias('openAccessType'), 
                		wosOutput3.abstract.alias('abstract'),
                  		wosOutput3.fundingText.alias('fundingText'),        
                  		wosOutput3.citedReferenceCount.alias('citedReferenceCount'),      
                  		wosOutput3.full_address.alias('full_address'),     
                  		wosOutput3.reprintAddress.alias('reprintAddress'),   
                  		wosOutput3.articleNumber.alias('articleNumber'),
                  		wosOutput3.publicationYear.alias('publicationYear'),
                  		wosOutput3.publicationDate.alias('publicationDate'),
                  		wosOutput3.volume.alias('volume'),
                  		wosOutput3.issue.alias('issue'),
                  		wosOutput3.partNumber.alias('partNumber'),
                  		wosOutput3.supplement.alias('supplement'),
                  		wosOutput3.specialIssue.alias('specialIssue'),
                  		wosOutput3.earlyAccessDate.alias('earlyAccessDate'),
                  		wosOutput3.startPage.alias('startPage'),
                  		wosOutput3.endPage.alias('endPage'),
                  		wosOutput3.numberOfPages.alias('numberOfPages'),
                  		wosOutput3.publisherCity.alias('publisherCity'),
                  		wosOutput3.publisherAddress.alias('publisherAddress'),
                  		wosOutput3.publisher.alias('publisher'),
                  		wosOutput3.keywordsPlus.alias('keywordsPlus'),
	                    wosOutput3.conferenceDate.alias('conferenceDate'),
       		            wosOutput3.conferenceSponsor.alias('conferenceSponsor'),
             		    wosOutput3.conferenceHost.alias('conferenceHost'),
                  		wosOutput3.conferenceTitle.alias('conferenceTitle'),
	                    wosOutput3.documentType.alias('documentType'),
      	                wosOutput3.RIDs.alias('RIDs'),
      	                wosOutput3.ORCID.alias('ORCID'),
      	                wosOutput3.standardNames.alias('standardNames'),
      	                wosOutput3.authors.alias('authors'),
                  		wosOutput3.emailAddress.alias('emailAddress'),
                  		wosOutput3.paperTitle.alias('paperTitle'), 
                        wosOutput3.journalTitle.alias('journalTitle'),
                        wosOutput3.journalAbbrev.alias('journalAbbrev'), 
                        wosOutput3.journalISO.alias('journalISO'),
                        	wosIdPlus4.issn.alias('issn'),
                            wosIdPlus4.doi.alias('doi'),
                            wosIdPlus4.eissn.alias('eissn'), 
                            wosIdPlus4.isbn.alias('isbn'),
                            wosIdPlus4.pmid.alias('pmid')
                         )
                         
wosOutput5 = wosOutput4.join(wosCoLo2,  wosOutput4['wosID'] == wosCoLo2['wosID'], how='full') \
         				.select(coalesce(wosOutput4.wosID, wosCoLo2.wosID).alias('wosID'), 
         				wosOutput4.isOpenAccess.alias('isOpenAccess'),
                        wosOutput4.openAccessType.alias('openAccessType'), 
                   		wosOutput4.abstract.alias('abstract'),
                  		wosOutput4.fundingText.alias('fundingText'),        
                  		wosOutput4.citedReferenceCount.alias('citedReferenceCount'),      
                  		wosOutput4.full_address.alias('full_address'),     
                  		wosOutput4.reprintAddress.alias('reprintAddress'),   
                  		wosOutput4.articleNumber.alias('articleNumber'),
                  		wosOutput4.publicationYear.alias('publicationYear'),
                  		wosOutput4.publicationDate.alias('publicationDate'),
                  		wosOutput4.volume.alias('volume'),
                  		wosOutput4.issue.alias('issue'),
                  		wosOutput4.partNumber.alias('partNumber'),
                  		wosOutput4.supplement.alias('supplement'),
                  		wosOutput4.specialIssue.alias('specialIssue'),
                  		wosOutput4.earlyAccessDate.alias('earlyAccessDate'),
                  		wosOutput4.startPage.alias('startPage'),
                  		wosOutput4.endPage.alias('endPage'),
                  		wosOutput4.numberOfPages.alias('numberOfPages'),
                  		wosOutput4.publisherCity.alias('publisherCity'),
                  		wosOutput4.publisherAddress.alias('publisherAddress'),
                  		wosOutput4.publisher.alias('publisher'),
                  		wosOutput4.keywordsPlus.alias('keywordsPlus'),
	                    wosOutput4.conferenceDate.alias('conferenceDate'),
       		            wosOutput4.conferenceSponsor.alias('conferenceSponsor'),
             		    wosOutput4.conferenceHost.alias('conferenceHost'),
                  		wosOutput4.conferenceTitle.alias('conferenceTitle'),
	                    wosOutput4.documentType.alias('documentType'),
      	                wosOutput4.RIDs.alias('RIDs'),
      	                wosOutput4.ORCID.alias('ORCID'),
      	                wosOutput4.standardNames.alias('standardNames'),
      	                wosOutput4.authors.alias('authors'),
                  		wosOutput4.emailAddress.alias('emailAddress'),
                  		wosOutput4.paperTitle.alias('paperTitle'), 
                        wosOutput4.journalTitle.alias('journalTitle'),
                        wosOutput4.journalAbbrev.alias('journalAbbrev'), 
                        wosOutput4.journalISO.alias('journalISO'),
                        wosOutput4.issn.alias('issn'),
                        wosOutput4.doi.alias('doi'),
                        wosOutput4.eissn.alias('eissn'), 
                        wosOutput4.isbn.alias('isbn'),
                        wosOutput4.pmid.alias('pmid'),
                        	wosCoLo2.conferenceLocation.alias('conferenceLocation')
                        )
                        
#SIXTH JOIN

wosOutput6 = wosOutput5.join(wosFo3,  wosOutput5['wosID'] == wosFo3['wosID'], how='full') \
         				.select(coalesce(wosOutput5.wosID, wosFo3.wosID).alias('wosID'), 
         				wosOutput5.isOpenAccess.alias('isOpenAccess'),
                        wosOutput5.openAccessType.alias('openAccessType'), 
                        wosOutput5.abstract.alias('abstract'),
                  		wosOutput5.fundingText.alias('fundingText'),        
                  		wosOutput5.citedReferenceCount.alias('citedReferenceCount'),      
                  		wosOutput5.full_address.alias('full_address'),     
                  		wosOutput5.reprintAddress.alias('reprintAddress'),   
                  		wosOutput5.articleNumber.alias('articleNumber'),
                  		wosOutput5.publicationYear.alias('publicationYear'),
                  		wosOutput5.publicationDate.alias('publicationDate'),
                  		wosOutput5.volume.alias('volume'),
                  		wosOutput5.issue.alias('issue'),
                  		wosOutput5.partNumber.alias('partNumber'),
                  		wosOutput5.supplement.alias('supplement'),
                  		wosOutput5.specialIssue.alias('specialIssue'),
                  		wosOutput5.earlyAccessDate.alias('earlyAccessDate'),
                  		wosOutput5.startPage.alias('startPage'),
                  		wosOutput5.endPage.alias('endPage'),
                  		wosOutput5.numberOfPages.alias('numberOfPages'),
                  		wosOutput5.publisherCity.alias('publisherCity'),
                  		wosOutput5.publisherAddress.alias('publisherAddress'),
                  		wosOutput5.publisher.alias('publisher'),
                  		wosOutput5.keywordsPlus.alias('keywordsPlus'),
	                    wosOutput5.conferenceDate.alias('conferenceDate'),
       		            wosOutput5.conferenceSponsor.alias('conferenceSponsor'),
             		    wosOutput5.conferenceHost.alias('conferenceHost'),
                  		wosOutput5.conferenceTitle.alias('conferenceTitle'),
	                    wosOutput5.documentType.alias('documentType'),
      	                wosOutput5.RIDs.alias('RIDs'),
      	                wosOutput5.ORCID.alias('ORCID'),
      	                wosOutput5.standardNames.alias('standardNames'),
      	                wosOutput5.authors.alias('authors'),
                  		wosOutput5.emailAddress.alias('emailAddress'),
                  		wosOutput5.paperTitle.alias('paperTitle'), 
                        wosOutput5.journalTitle.alias('journalTitle'),
                        wosOutput5.journalAbbrev.alias('journalAbbrev'), 
                        wosOutput5.journalISO.alias('journalISO'),
                        wosOutput5.issn.alias('issn'),
                        wosOutput5.doi.alias('doi'),
                        wosOutput5.eissn.alias('eissn'), 
                        wosOutput5.isbn.alias('isbn'),
                        wosOutput5.pmid.alias('pmid'),
                        wosOutput5.conferenceLocation.alias('conferenceLocation'),
              				wosFo3.fundingOrgs.alias('fundingOrgs')
              			)            
                        
def clean_text(c):
  c = regexp_replace(c, '"' , '')
  c = regexp_replace(c, '\\\\' , '')
  c = regexp_replace(c, '$s/"//g', '')
  return c
  
  
#CLEAN TEXT
wosOutput7 = wosOutput6.select(
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
                        
                        
wosOutput7.coalesce(100).write.option("header","True") \
                               .option("sep","\t") \
                               .option("quoteAll", False) \
                               .option("emptyValue", None) \
                               .option("nullValue", None)\
                               .mode("overwrite") \
                               .csv('/WoSraw_2020_all/nodes2')                       
                        




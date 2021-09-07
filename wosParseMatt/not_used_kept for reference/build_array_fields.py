
#BUILD AUTHOR ARRAY
wos5 = WoS.select(WoS.UID.alias("wosID"), WoS.static_data.contributors.contributor.alias("author"))

wos6 = wos5.withColumn("author", explode("author")).select("*", col("author")["name"]["_seq_no"].alias("seq_no"),
                                                                col("author")["name"]["full_name"].alias("fullname"),
                                                                col("author")["name"]["_r_id"].alias("RIDs"), 
                                                                col("author")["name"]["_orcid_id"].alias("ORCID"))

wos_author = wos6.select("wosID", concat_ws(" ", wos6.fullname, wos6.seq_no).alias("author"), 
                                  concat_ws(" ", wos6.RIDs,     wos6.seq_no).alias("RIDs"), 
                                  concat_ws(" ", wos6.ORCID,    wos6.seq_no).alias("ORCID")
                        )


wos_auth2 = wos_author.groupBy("wosID").agg(sort_array(collect_list("author")).alias("authors"),
                                            sort_array(collect_list("RIDs")).alias("RIDs"),
                                            sort_array(collect_list("ORCID")).alias("ORCID"))  
                                            

wos_auth2 = wos_auth2.select(
   col('wosID'),
   concat_ws("| ", col('RIDs')).alias('RIDs'),
   concat_ws("| ", col('ORCID')).alias('ORCID'), 
   concat_ws("| ",col('authors')).alias('authors'),
   size('authors').alias('count')
)

wos_auth2 = wos_auth2.drop('count')

wos_auth2.show(10, truncate=False)

##########################################################################################################
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

wosTitle4 = item.join(source,      item['wosID'] == source['id2']) \
                .join(journalAbbr, item['wosID'] == journalAbbr['id3']) \
                .join(journalISO,  item['wosID'] == journalISO['id4'])

wosTitle4 = wosTitle4.drop('id2', 'id3', 'id4')

wosTitle4.show(10, truncate = True)


##########################################################################################################
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

##########################################################################################################
#BUILD CONFERENCE LOCATION

wosCoLo = WoS.select(WoS.UID.alias("wosID"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_city.alias("conf_city"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_host.alias("conf_host"),
                    WoS.static_data.summary.conferences.conference.conf_locations.conf_location.conf_state.alias("conf_state")
                    )

wosCoLo2 = wosCoLo.withColumn('conferenceLocation', concat_ws(", ", wosCoLo.conf_city, wosCoLo.conf_host, wosCoLo.conf_state))

##########################################################################################################
#BUILD FUNDING ORG String

wosFo = WoS.select(WoS.UID.alias("wosID"),
                WoS.static_data.fullrecord_metadata.fund_ack.grants.grant.grant_agency[0].alias('fundingOrgs'))


wosFo2 = wosFo.withColumn("fundingOrgs", col("fundingOrgs").getField("_VALUE"))

wosFo3 = wosFo2.withColumn("fundingOrgs", concat_ws("| ", wosFo2.fundingOrgs))


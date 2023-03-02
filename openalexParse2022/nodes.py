import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, lit, expr, udf, length, concat_ws, concat
from pyspark.sql.types import *
from datetime import datetime as dt

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

csv_loc = '/OpenAlex_202211/jg/'

clean_text = udf(lambda x: x.replace('\n','').replace('\t',' ').replace('\r','').replace('\x08','').strip() if x is not None else None, StringType())
oa_id = udf(lambda x: x.split(".org/")[-1] if x is not None else None, StringType())
orc_id = udf(lambda x: x.split(".org/")[-1] if x is not None else None, StringType())
scopus_id = udf(lambda x: x.split("authorID=")[-1].split('&')[0] if x is not None else None, IntegerType())
twitter_id = udf(lambda x: x.split("twitter.com/")[-1] if x is not None else None, StringType())
emp_list = udf(lambda x: x if len(x) > 0 else None)
name_lr = udf(lambda x: x.lower() if x is not None else None, StringType())
pipecat = udf(lambda x: '|' + x + '|' if x is not None and x != "" else None, StringType())

start = dt.now()

#########################
# dataframe for authors #
#########################
df = spark.read.load('/OpenAlex_202211/parquets/authors/')\
                    .withColumn("oa_author_id", oa_id(col("id")))

# authors node
authors = df.select(\
                    # identifers
                    col("oa_author_id"),\
                    col("ids.mag").alias("mag_id"),\
                    orc_id(col("ids.orcid")).alias("orc_id"),\
                    scopus_id(col("ids.scopus")).alias("scopus_id"),\
                    twitter_id(col("ids.twitter")).alias("twitter_id"),\
                    # counts
                    col("works_count"),\
                    col("cited_by_count"),\
                    # dates
                    col("created_date"),\
                    col("updated_date"),\
                    # names
                    name_lr(col("display_name")).alias("name"),\
                    emp_list(col("display_name_alternatives")).alias("alternative_names"),\
                    # misc
                    col("works_api_url")\
                )

authors.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'author_nodes/')

# authors to concept edge
auth2con = df.select(col("oa_author_id"),\
                        explode(col("x_concepts")))\
                .select(col("oa_author_id"),\
                        oa_id(col("col.id")).alias("oa_concept_id")\
                )

auth2con.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'auth2con_edges/')

#################################
# dataframes for concepts nodes #
#################################

# concepts first load
df = spark.read.load('/OpenAlex_202211/parquets/concepts/')\
                        .withColumn("oa_concept_id", oa_id(col("id")))

concepts = df.select(\
                        #identifers
                        col("oa_concept_id"),\
                        col("wikidata").alias("wikidata_id"),\
                        col("ids.mag").alias("mag_id"),\
                        col("ids.wikipedia").alias("wikipedia_id"),\
                        concat_ws('|',col("ids.umls_aui")).alias("umls_aui_id"),\
                        concat_ws('|',col("ids.umls_cui")).alias("umls_cui_id"),\
                        # counts
                        col("works_count"),\
                        col("cited_by_count"),\
                        # dates
                        col("created_date"),\
                        col("updated_date"),\
                        # names
                        name_lr(col("display_name")).alias("name"),\
                        # misc
                        col("level"),\
                        col("description")\
                    )\
                    .withColumn('umls_aui_id', pipecat(col("umls_aui_id")))\
                    .withColumn('umls_cui_id', pipecat(col("umls_cui_id")))

concepts.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'concept_nodes/')

# concepts to concepts (ancestors) edge
ancestors = df.select(\
                        col("oa_concept_id"),\
                        explode(col("ancestors")))\
                .select(\
                        col("oa_concept_id").alias("descendant_concept_id"),\
                        oa_id(col("col.id")).alias("ancestor_concept_id")\
                )

ancestors.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'con2ancestor_edges/')

# concepts to concepts (related) edge
related = df.select(\
                    col("oa_concept_id"),\
                    explode(col("related_concepts")))\
            .select(\
                    col("oa_concept_id").alias("oa_concept_id"),\
                    oa_id(col("col.id")).alias("related_concept_id")\
            )

related.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode("overwrite").csv(csv_loc+"con2related_edges/")

#####################################
# dataframes for institutions nodes #
#####################################

# institutions first load
df = spark.read.load("/OpenAlex_202211/parquets/institutions/")\
                        .withColumn("oa_institution_id",oa_id(col("id")))

institutions = df.select(\
                        # identifiers
                        col("oa_institution_id"),\
                        col("ror"),\
                        col("ids.wikidata").alias("wikidata_id"),\
                        col("ids.grid").alias("grid_id"),\
                        col("ids.mag").alias("mag_id"),\
                        col("ids.wikipedia").alias("wikipedia_id"),\
                        # counts
                        col("works_count"),\
                        col("cited_by_count"),\
                        # dates
                        col("created_date"),\
                        col("updated_date"),\
                        # names
                        name_lr(col("display_name")).alias("name"),\
                        concat_ws('|',col("display_name_alternatives")).alias("alternative_names"),\
                        # location
                        col("geo.city").alias("city"),\
                        col("geo.country").alias("country"),\
                        col("geo.geonames_city_id").alias("geonames_city_id"),\
                        col("geo.latitude").alias("latitude"),\
                        col("geo.longitude").alias("longitude"),\
                        col("geo.region").alias("region"),\
                        col("country_code"),\
                        # misc
                        col("homepage_url"),\
                        col("type"),\
                        concat_ws('|',col("display_name_acronyms")).alias("display_name_acronyms")
                        )\
                        .withColumn('alternative_names', pipecat(col("alternative_names")))\
                        .withColumn('display_name_acronyms', pipecat(col("display_name_acronyms")))

institutions.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'institution_nodes/')

# institutions to institutions (associated) edge
associated = df.select(\
                        col("oa_institution_id"),\
                        explode(col("associated_institutions")))\
                .select(\
                        col("oa_institution_id").alias("oa_institution_id"),\
                        oa_id(col("col.id")).alias("associated_institution_id")\
                )

associated.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'institution_associated_edges/')

# institutions to concepts edge
inst_2_con = df.select(\
                        col("oa_institution_id"),\
                        explode(col("x_concepts")))\
                .select(\
                        col("oa_institution_id").alias("oa_institution_id"),\
                        oa_id(col("col.id")).alias("oa_concept_id")\
                )
inst_2_con.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'institution2concept_edges')

##############################
# dataframes for venue nodes #
##############################

# first venues load
df = spark.read.load('/OpenAlex_202211/parquets/venues')\
                        .withColumn("oa_venues_id", oa_id(col("id")))

venues = df.select(\
                    # identifiers
                    col("oa_venues_id"),\
                    col("issn_l"),\
                    col("ids.mag").alias("mag_id"),\
                    concat_ws('|',col("ids.issn")).alias("issns"),\
                    col("ids.fatcat").alias("fatcat_id"),\
                    col("ids.wikidata").alias("wikidata_id"),\
                    # counts 
                    col("works_count"),\
                    col("cited_by_count"),\
                    # date
                    col("created_date"),\
                    col("updated_date"),\
                    # names
                    name_lr(col("display_name")).alias("name"),\
                    concat_ws('|',col("alternate_titles")).alias("alternative_titles"),\
                    col("abbreviated_title"),\
                    # misc
                    col("publisher"),\
                    col("homepage_url"),\
                    col("apc_usd"),\
                    col("is_oa"),\
                    col("is_in_doaj")\
                    )\
                    .withColumn("issns", pipecat(col("issns")))\
                    .withColumn("alternative_titles", pipecat(col("issns")))

venues.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'venue_nodes/')

# venues to concepts edge
ven2con = df.select(\
                    col("oa_venues_id"),\
                    explode(col("x_concepts")))\
            .select(\
                    col("oa_venues_id").alias("oa_venues_id"),\
                    oa_id(col("col.id").alias("oa_concept_id"))\
            )

ven2con.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'venue2concept_edges/')

# venue societies nodes
venue_societies = df.select(\
                        col("societies.url"),\
                        col("societies.organization")
                        )\
                    .withColumn("url",emp_list(col("url")))\
                    .withColumn("organization",emp_list(col("organization")))\
                    .na.drop(subset=['url','organization'],how='all')

venue_societies.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'society_nodes/')

# venues to societies edge
ven2soc = df.select(\
                    col("oa_venues_id"),\
                    explode(col("societies")))\
                .select(\
                        col("oa_venues_id").alias("oa_venue_id"),\
                        col("col.url").alias("society_url")\
                )

ven2soc.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'venue2society_edges/')

#############################
# dataframe for works table #
#############################
df = spark.read.load('/OpenAlex_202211/parquets/works/')\
                        .withColumn('oa_work_id',oa_id(col("id")))

works = df.select(\
                # identifiers
                col("oa_work_id"),\
                col("doi"),\
                col("ids.mag").alias("mag_id"),\
                col("ids.pmid").alias("pmid"),\
                # counts
                col("cited_by_count"),\
                # dates
                col("created_date"),\
                col("updated_date"),\
                col("publication_date"),\
                # names
                name_lr(col("display_name")).alias("name"),\
                clean_text(col("title")).alias("title"),\
                # open access
                col("open_access.is_oa"),\
                col("open_access.oa_status"),\
                col("open_access.oa_url"),\
                # misc
                col("type"),\
                col("is_retracted"),\
                col("is_paratext"),\
                col("cited_by_api_url")
                )

works.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'work_nodes/')

# work to venue (host) edge
hosts = df.select(\
                    col("oa_work_id").alias("oa_work_id"),\
                    oa_id(col("host_venue.id")).alias("host_oa_venue_id"))\
            .na.drop(subset=['host_oa_venue_id'], how='all')

hosts.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'work2venue_edges/')

# authorships
authorships = df.select(\
                        col("oa_work_id"),\
                        explode(col("authorships"))\
                    )

# work to author (authorship) edge
work2auth = authorships.select(\
                                col("oa_work_id").alias("oa_work_id"),\
                                oa_id(col("col.author.id")).alias("oa_author_id")\
                        )

work2auth.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'work2auth_edges/')

# work to institution (authorship) edge
work2inst = authorships.select(\
                                col("oa_work_id").alias("oa_work_id"),\
                                explode(col("col.institutions")))\
                        .select(\
                                col("oa_work_id"),\
                                oa_id(col("col.id")).alias("oa_institution_id")\
                        )

work2inst.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'work2institution_edges')

# work to concepts edge
work2con = df.select(\
                        col("oa_work_id"),\
                        explode(col("concepts")))\
                .select(\
                        col("oa_work_id").alias("oa_work_id"),\
                        oa_id(col("col.id")).alias("oa_concept_id"),\
                        col("col.score")
                )

work2con.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'work2con_edges/')

# work to work (referenced) edge
workRef = df.select(\
                        col("oa_work_id"),\
                        explode("referenced_works"))\
                .select(\
                        col("oa_work_id").alias("oa_work_id"),\
                        oa_id(col("col")).alias("oa_referenced_work_id")\
                )

workRef.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'workReferenced_edges/')

# work to work (related) edge
workRel = df.select(\
                    col("oa_work_id"),\
                    explode(col("related_works")))\
            .select(\
                    col("oa_work_id").alias("oa_work_id"),\
                    oa_id(col("col")).alias("oa_related_work_id")\
            )

workRel.write.option('header','True')\
                        .option('sep','\t').option('quote','\u0000').option('nullValue',None)\
                        .mode('overwrite').csv(csv_loc+'workRelated_edges/')

end = dt.now()
print(end - start)
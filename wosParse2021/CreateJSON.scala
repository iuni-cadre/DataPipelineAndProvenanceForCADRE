import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import com.databricks.spark.xml._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{Row, Column, SQLContext}
import org.apache.spark.sql.functions._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
//sqlContext.sql("set spark.sql.caseSensitive=true")
spark.conf.set('spark.sql.caseSensitive', 'True')

print("Reading parquet files")
val WoS = spark.read.format("parquet").load("/WoSraw_2021/parquet/*.parquet")

print("\n\nCreating WoS2 dataframe")
val WoS2 = WoS.select("UID","dynamic_data.cluster_related.identifiers.identifier","static_data.summary.doctypes.doctype",
                      "static_data.summary.names.name","static_data.summary.titles","static_data.summary.conferences.conference",
                      "static_data.fullrecord_metadata.addresses.address_name","static_data.fullrecord_metadata.abstracts.abstract.abstract_text",
                      "static_data.fullrecord_metadata.keywords.keyword",
                      "static_data.fullrecord_metadata.references.reference",
                      "static_data.summary.pub_info","static_data.summary.publishers","static_data.fullrecord_metadata.fund_ack.grants.grant")

print("\n\nWoS2 schema")
WoS2.printSchema

//print("\n\nWriting WoS2 to JSON") 
//WoS2.write.mode(SaveMode.Overwrite).json("/WoSraw_2021/json")

print("\n\nFirst 10 records in WoS2")
WoS2.show(10, false)

print("\n\nShow column nulls count")
WoS2.select(WoS2.columns.map(colName => {
    count(when(col(colName).isNull, true)) as s"${colName}_nulls_count"
  }): _*)
.show(10)

print("\n\nComputing docTypeCount")
val docTypeCount = WoS2.groupBy("doctype").count()
docTypeCount.show(docTypeCount.count.toInt, false)

print("\n\n Describe summary === count")
WoS2.describe().filter($"summary" === "count").show

val edges = WoS2.select("UID","pub_info._pubyear","reference.uid").withColumn("cited", explode($"uid"))
//edges.show(false)
edges.describe().filter($"summary" === "count").show



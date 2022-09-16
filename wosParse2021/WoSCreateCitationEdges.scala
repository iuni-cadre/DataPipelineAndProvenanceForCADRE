import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import com.databricks.spark.xml._
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.not

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
sqlContext.sql("set spark.sql.caseSensitive=true")

val WoS = spark.read.format("parquet").load("/WoSraw_2021/parquet/*.parquet")

val WoS2 = WoS.select("UID","dynamic_data.cluster_related.identifiers.identifier","static_data.summary.doctypes.doctype",
                      "static_data.summary.names.name","static_data.summary.titles","static_data.summary.conferences.conference",
                      "static_data.fullrecord_metadata.addresses.address_name", "static_data.fullrecord_metadata.reprint_addresses",
                      "static_data.fullrecord_metadata.abstracts.abstract.abstract_text",
                      "static_data.fullrecord_metadata.keywords.keyword","static_data.fullrecord_metadata.references.reference",
                      "static_data.contributors.contributor","static_data.summary.EWUID",
                      "static_data.summary.pub_info","static_data.summary.publishers","static_data.fullrecord_metadata.fund_ack")

val WoSref = WoS2.select("UID","reference.uid").toDF("citing","refList").withColumn("cited", explode(col("refList")))
WoSref.select("citing","cited").where($"cited".contains("WOS:") && not($"cited".contains(".")))
    .coalesce(100).write.mode(SaveMode.Overwrite)
    .format("com.databricks.spark.csv")
    .option("header", "true")//.option("compression", "gzip")
    .save("/WoSraw_2021/edges/refEdgesFiltered.csv")


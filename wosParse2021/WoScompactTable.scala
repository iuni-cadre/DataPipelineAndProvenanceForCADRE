import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import com.databricks.spark.xml._
import org.apache.spark.sql.SaveMode

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
sqlContext.sql("set spark.sql.caseSensitive=true") 
spark.sparkContext.uiWebUrl

//val sqlContext = new SQLContext(sc)
val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "REC")
  //.schema(customSchema)
  .load("/WoSraw_2021/*.xml.gz")

df.show(2)
df.printSchema()

df.write.mode(SaveMode.Overwrite).parquet("/WoSraw_2021/parquet")

export HADOOP_USER_NAME=hdfs
export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark/conf/yarn-conf
export SPARK_HOME=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark

spark-shell \
  --master yarn \
  --packages \
    com.databricks:spark-xml_2.11:0.12.0,graphframes:graphframes:0.7.0-spark2.4-s_2.11 \
  --driver-memory 8G \
  --num-executors 7 \
  --executor-memory 28G \
  --executor-cores 4 \
  --conf spark.driver.maxResultSize=8g \
  -i CreateJSON.scala

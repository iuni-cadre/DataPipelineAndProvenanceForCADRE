module load intel
module swap python/3.6.8

export HADOOP_USER_NAME=hdfs
export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark/conf/yarn-conf
export SPARK_HOME=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark

spark-shell \
  --master yarn \
  --driver-memory 8G \
  --num-executors 21 \
  --executor-cores 4 \
  --executor-memory 28G \
  --conf spark.yarn.executor.memoryOverheadFactor=0.1 \
  --conf spark.driver.maxResultSize=8g \
  -i WoSCreateCitationEdges.scala

echo "Return code from spark-submit is: " $?

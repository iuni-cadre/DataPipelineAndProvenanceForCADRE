export HADOOP_USER_NAME=hdfs
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark

module swap python/3.6.8

pip install --user Toree
export PATH=~/.local/bin:$PATH
jupyter toree install --spark_home=$SPARK_HOME --user --spark_opts="--packages com.databricks:spark-xml_2.11:0.5.0 --driver-memory 8G --executor-memory 15G --executor-cores 6 --conf spark.driver.maxResultSize=8g"
jupyter kernelspec list

jupyter serverextension enable --py jupyterlab
cd /gpfs/projects/UITS/IUNI/IMAGENE/
jupyter notebook --no-browser --port=8000 --ip=149.165.230.163

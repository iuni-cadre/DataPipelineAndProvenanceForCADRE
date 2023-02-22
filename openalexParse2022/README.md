### MAG to OpenAlex

OpenAlex stands in as a replacement of OpenAlex.


### Environment Setup

1. ssh to iuni2

2. load carbonate native python 3.6.8
```module load python/.3.6.8```

3. export environmental variables
```
export HADOOP_USER_NAME=hdfs
export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark/conf/yarn-conf
export SPARK_HOME=/opt/cloudera/parcels/CDH-6.3.1-1.cdh6.3.1.p0.1470567/lib/spark
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
```

### Process

1) Get a sample file transformed to parquet from JSON.

- This is because of how long JSON takes to load at the scale of OpenAlex

- Using ```jsonSchema.py``` against largest file of entity to get a good schema sample

2) Using Schema to load in all JSON and transform to parquet

- Using ```json2parquet2.py```

3) Run ```node.py``` to extract all TSVs for edges and nodes for OpenAlex
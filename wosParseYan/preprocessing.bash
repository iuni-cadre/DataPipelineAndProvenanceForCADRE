ssh iuni2.carbonate.uits.iu.edu

export HADOOP_USER_NAME=hdfs
export PATH=$PATH:~/.local/bin

cd /N/project/iuni_cadre/WoS_18_19
unzip 'ESCI 2005_2019/'*.zip' -d xmlRaw
unzip 'CORE 1900-2019'/'*.zip' -d xmlRaw

hdfs dfs -mkdir /WoSraw/
hdfs dfs -copyFromLocal xmlRaw/ /WoSraw/WoS2019
hdfs dfs -ls /
hdfs dfs -copyToLocal <hdfs_input_file_path> <output_path>

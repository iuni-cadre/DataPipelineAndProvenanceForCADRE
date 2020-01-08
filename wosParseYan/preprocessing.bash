ssh iuni2.carbonate.uits.iu.edu

export HADOOP_USER_NAME=hdfs
export PATH=$PATH:~/.local/bin

cd /N/project/iuni_cadre/wos_raw/wos_17_18
unzip ESCI_2005_2019_Annuals/'*.zip' -d xmlRaw
unzip CORE_1900-2018/'*.zip' -d xmlRaw

hdfs dfs -mkdir /WoSraw/
hdfs dfs -copyFromLocal xmlRaw/ /WoSraw/
hdfs dfs -ls /
hdfs dfs -copyToLocal <hdfs_input_file_path> <output_path>

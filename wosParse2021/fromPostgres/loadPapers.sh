#!/bin/bash

allFiles=`ls /N/project/iuni_cadre/wos/wos_jg_2021/fromSpark/nodes/*.csv`
delim="E'\t'"

for i in $allFiles
do
cmd='psql -U cadre_admin -d core_data -c "\COPY wos_jg_21.wos_jg FROM '$i' DELIMITER '$delim' CSV HEADER;"'
echo $cmd
eval $cmd
done

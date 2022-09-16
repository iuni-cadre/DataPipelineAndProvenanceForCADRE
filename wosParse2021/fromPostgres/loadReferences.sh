#!/bin/bash

allFiles=`ls /N/project/iuni_cadre/wos/wos_jg_2021/fromSpark/edges/*.csv`

for i in $allFiles
do
cmd='psql -U cadre_admin -d core_data -c "\COPY wos_jg_21.reference FROM '$i' WITH (format csv, header TRUE);"'
eval $cmd
done

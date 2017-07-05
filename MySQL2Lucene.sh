#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup

NOW=$(date +"%Y%m%d")

sbt "run-main org.bireme.dcdup.MySQL2Pipe -host=basalto03.bireme.br -user=fi-admin -pswd=fi-admin@bireme -dbnm=fi-admin -sql=LILACS_Sas.sql -pipe=pipeOut_$NOW.txt  -jsonField=text,_f -repetitiveField=title"
sbt "run-main org.bireme.dcdup.Pipe2Lucene /home/javaapps/DeDup/work/lilacs_Sas /home/javaapps/DeDup/work/configLILACS_Sas_Seven.cfg utf-8 pipeOut_$NOW.txt"

rm pipeOut_$NOW.txt

cd -

#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup
sbt "run-main org.bireme.dcdup.MySQL2Pipe -host=basalto03.bireme.br -user=fi-admin -pswd=fi-admin@bireme -dbnm=fi-admin -sql=LILACS_Sas.sql -pipe=pipeOut.txt -jsonField=text,_f -repetitiveField=title"
cd -

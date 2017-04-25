#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup
sbt "run-main org.bireme.dcdup.MySQL2Pipe -host=basalto03.bireme.br -user=fi-admin-poc -pswd=fiadmin@2016 -dbnm=fi-admin-poc -sql=LILACS_Sas.sql -pipe=pipeOut.txt -jsonField=text,_f -repetitiveField=title"
cd -sbt "

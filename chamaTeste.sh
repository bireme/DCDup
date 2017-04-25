#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup
sbt "run-main org.bireme.dcdup.MySQL2Pipe -host=basalto08.bireme.br -user=fi-admin-user -pswd=fi@admin2015 -dbnm=fi_admin_tst -sql=LILACS_Sas.sql -pipe=pipeOut.txt -jsonField=text,_f -repetitiveField=title"
cd -

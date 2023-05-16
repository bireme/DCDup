#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup || exit

bin/MySQL2Pipe.sh -host=osmio02.bireme.br -port=6612 -user=acesso_fiadmin -pswd=fi_admin@BIR -dbnm=fi_admin -sqls=sql/LILACS_Sas.sql,sql/LILACS_Sas_ingles.sql -pipe=lilacs_Sas.pipe -schema=configs/configLILACS_Sas_Seven.cfg

cd -

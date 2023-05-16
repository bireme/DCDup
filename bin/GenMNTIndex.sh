#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup || exit

bin/MySQL2Lucene.sh -host=osmio02.bireme.br -port=6612 -user=acesso_fiadmin -pswd=fi_admin@BIR -dbnm=fi_admin -sqls=/home/javaapps/sbt-projects/DCDup/sql/LILACS_MNT.sql,/home/javaapps/sbt-projects/DCDup/sql/LILACS_MNT_ingles.sql -index=indexes/lilacs_MNT -schema=configs/configLILACS_MNT_Four.cfg

cd -

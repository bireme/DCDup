#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup || exit

bin/MySQL2Pipe.sh -host=osmio02.bireme.br -port=6612 -user=acesso_fiadmin -pswd=fi_admin@BIR -dbnm=fi_admin -sqls=/home/javaapps/sbt-projects/DCDup/sql/LILACS_MNTam.sql,/home/javaapps/sbt-projects/DCDup/sql/LILACS_MNTam_ingles.sql -pipe=lilacs_MNTam.pipe

cd -

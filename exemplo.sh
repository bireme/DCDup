#!/bin/bash

DCDUP_HOME=/home/heitor/sbt-projects/DCDup
#DCDUP_HOME=/home/javaapps/sbt-projects/DCDup

java -cp $DCDUP_HOME/target/scala-2.12/DCDup-assembly-0.1.0.jar org.bireme.dcdup.WebDoubleCheckDuplicated LILACSLike_Sas.txt iso-8859-1 http://ts10vm.bireme.br:8180/DeDup/services lilacs_Sas LILACS_Sas_Seven outdup1.txt outdup2.txt outnodup.txt utf-8

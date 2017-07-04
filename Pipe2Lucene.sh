#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup

sbt "run-main org.bireme.dcdup.Pipe2Lucene $1 $2 $3 $4 $5 $6"

cd -

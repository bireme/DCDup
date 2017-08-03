#!/bin/bash

if [ "$#" -ne "6" ]
  then
    echo 'LocalDuplicatedCheck shell looks for duplicated documents in a piped file.'
    echo
    echo 'Parameters:'
    echo '   <schemaFile> - DeDup schema file'
    echo '   <schemaFileEncoding> - schema file character encoding'
    echo '   <pipeFile> - DeDup piped input file'
    echo '   <pipeFileEncoding> - pipe file character encoding'
    echo '   <outDupFile> - duplicated records found in pipe file'
    echo '   <outDupEncoding> - output file character encoding'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup
sbt "run-main org.bireme.dcdup.LocalCheckDuplicated $1 $2 $3 $4 $5 $6"
cd -

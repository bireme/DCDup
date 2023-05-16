#!/bin/bash

if [ "$#" -lt "4" ]
  then
    echo 'SelfCheckDuplicated shell looks for duplicated documents in a piped file.'
    echo
    echo 'Parameters:'
    echo '   -pipe=<pipeFile> - DeDup piped input file'
    echo '   -schema=<schemaFile> - DeDup schema name'
    echo '   -outDupFile=<outDupFile> - duplicated records found in pipe file'
    echo '   -outNoDupFile=<outNoDupFile> - no duplicated records between pipe file and itself'
    echo '   [-pipeEncoding=<pipeEncoding>] - DeDup pipe file encoding. Default is utf-8'
    echo '   [-schemaEncoding=<schemaFileEncoding>] - DeDup schema file encoding. Default is utf-8'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

java -cp jar/DCDup-assembly-0.1.0.jar org.bireme.dcdup.SelfCheckDuplicated $1 $2 $3 $4 $5 $6 $7

cd - || exit


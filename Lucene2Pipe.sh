#!/bin/bash

if [ "$#" -lt "4" ]
  then
    echo 'Lucene2Pipe shell uses a local DeDup index and a schema file to
    exports all of its documents .'
    echo 'usage: Pipe2Lucene'
    echo '   <indexName> - DeDup index from where the new documents will be retrieved'
    echo '   <schemaName> - DeDup schema file (describing document structure)'
    echo '   <schemaEncoding> - DeDup schema file encoding'
    echo '   <pipeFile> - output pipe file'
    echo '   [<pipeEncoding>] - output pipe file encoding. Optional parameter. Default is utf-8'
else
  if [ "$#" -eq "4" ]
    then sbt "run-main org.bireme.dcdup.Lucene2Pipe $1 $2 $3 $4"
  else
    sbt "run-main org.bireme.dcdup.Lucene2Pipe $1 $2 $3 $4 -encoding=$5"
  fi
fi

cd /home/javaapps/sbt-projects/DCDup
sbt "run-main org.bireme.dcdup.Lucene2Pipe $1 $2 $3 $4 $5"
cd -

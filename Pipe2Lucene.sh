#!/bin/bash

if [ "$#" -lt "4" ]
  then
    echo 'Pipe2Lucene shell takes a list of documents (defined by an schema'
    echo 'file) from a piped file and insert then into a local DeDup index.'
    echo 'usage: Pipe2Lucene'
    echo '   <indexName> - DeDup index where the new documents will be inserted'
    echo '   <schemaName> - DeDup schema file (describing document structure)'
    echo '   <schemaEncoding> - DeDup schema file encoding'
    echo '   <pipeFile> - pipe file generated by webpipe2lucene shell'
    echo '   [<pipeEncoding>] - pipe file encoding. Optional parameter. Default is utf-8'
else
  if [ "$#" -eq "4" ]
    then sbt "run-main org.bireme.dcdup.Pipe2Lucene $1 $2 $3 $4"
  else
    sbt "run-main org.bireme.dcdup.Pipe2Lucene $1 $2 $3 $4 -encoding=$5"
  fi
fi
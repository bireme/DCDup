#!/bin/bash

if [ ! "$#" -eq "5" ]
  then
    echo 'usage: webpipe2lucene <pipeFile> <pipeEncoding> <DeDupServiceBase> <indexName> <schemaName>'
    exit 1
fi

sbt "run-main org.bireme.dcdup.WebPipe2Lucene $1 $2 $3 $4 $5"

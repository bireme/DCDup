#!/bin/bash

if [ ! "$#" -lt "8" ]
  then
    echo 'WebDoubleCheckDuplicated shell takes a list of documents (defined by an schema'
    echo 'file) from a piped file and check then against to a DeDup index looking for'
    echo "duplicated files."
    echo
    echo 'usage: WebDoubleCheckDuplicated'
    echo '  <pipeFile> - DeDup piped input file'
    echo '  <pipeFileEncoding> - pipe file character encoding'
    echo '  <DeDupBaseUrl> - DeDup url service'
    echo '  <indexName> - DeDup index name'
    echo '  <schemaName> - DeDup schema name'
    echo '  <outDupFile1> - duplicated records found in pipe file'
    echo '  <outDupFile2> - duplicated records found between pipe file and Dedup index'
    echo '  <outNoDupFile> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)'
    echo '  [<outDupEncoding>] - output file character encoding. Optional parameter. Default is utf-8'
    exit 1
fi

sbt "run-main org.bireme.dcdup.WebDoubleCheckDuplicated $1 $2 $3 $4 $5 $6 $7 $8 $9"

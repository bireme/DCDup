#!/bin/bash

if [ "$#" -lt "4" ]
  then echo 'usage: pipe2lucene <indexPath> <schemaFile> <schemaEncoding> <pipeFile> [<pipeEncoding>]'
else
  if [ "$#" -eq "4" ]
    then sbt "run-main org.bireme.dcdup.Pipe2Lucene $1 $2 $3 $4"
  else
    sbt "run-main org.bireme.dcdup.Pipe2Lucene $1 $2 $3 $4 -encoding=$5"
  fi
fi

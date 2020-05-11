#!/bin/bash

JAVA_HOME=/usr/local/oracle-8-jdk
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "4" ]
  then
    echo 'Pipe2Lucene shell takes a list of documents (defined by an schema'
    echo 'file) from a piped file and insert them into a local DeDup index.'
    echo
    echo 'usage: Pipe2Lucene'
    echo '   -index=<indexPath> - DeDup index path'
    echo '   -schema=<schemaFile> - DeDup schema file'
    echo '   -pipe=<pipeFile> - pipe file'
    echo '   -pipeEncoding=<pipeEncoding> - pipe file encoding'
    echo
    echo '   [-schemaFileEncod=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8'
    echo '   [--append] - append documents to an existing DeDup index'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

NOW=$(date +"%Y%m%d%H%M%S")

java -cp target/scala-2.12/DCDup-assembly-0.1.0.jar org.bireme.dcdup.CheckPipeFile "$1" "$2" "$3" "$4" "$5" "$6" -good="good_$NOW.txt" -bad="bad_$NOW.txt"

echo "==> Please, see the ignored documents at bad_$NOW.txt"

java -cp target/scala-2.12/DCDup-assembly-0.1.0.jar org.bireme.dcdup.Pipe2Lucene "$1" "$2" "$3" "$4" "$5" "$6" -pipe="good_$NOW.txt" -pipeEncoding=utf-8

cd - || exit

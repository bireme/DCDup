#!/bin/bash

JAVA_HOME=/usr/local/oracle-8-jdk
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "3" ]
  then
    echo 'Pipe2Lucene shell takes a list of documents (defined by an schema'
    echo 'file) from a piped file and insert them into a local DeDup index.'
    echo
    echo 'usage: Pipe2Lucene'
    echo '   -index=<indexPath> - DeDup index path'
    echo '   -schema=<schemaFile> - DeDup schema file'
    echo '   -pipe=<pipeFile> - pipe file'
    echo
    echo '   [-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8'
    echo '   [-pipeEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8'
    echo '   [--append] - append documents to an existing DeDup index'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

NOW=$(date +"%Y%m%d%H%M%S")

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.CheckPipeFile "$1" "$2" "$3" "$4" "$5" "$6" -good="good_$NOW.txt" -bad="bad_$NOW.txt"

if [ "$?" -ne 0 ]; then
  echo 'Pipe file checking error'
  exit 1
fi

echo "==> Please, see the ignored documents at bad_$NOW.txt. Using good_$NOW.txt file."

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.Pipe2Lucene "$1" "$2" "$3" "$4" "$5" "$6" -pipe="good_$NOW.txt" -pipeEncoding=utf-8

cd - || exit

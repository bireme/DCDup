#!/bin/bash

JAVA_HOME=/usr/local/oracle-8-jdk
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "4" ]
  then
    echo 'Given a pipe file, creates two other files: the one whose lines follow a DeDup schema and'
    echo 'the other that dont. From the one that follows, creates another ones where each one, group'
    echo 'pipe lines that contain (i.e. are not empty) elements in the same position.'
    echo
    echo 'usage: SimilarPipes'
    echo '   -pipe=<pipeFile> - input piped file'
    echo '   -schema=<schemaFile> - DeDup schema file'
    echo '   -good=<file path> - file that contains piped lines following the schema'
    echo '   -bad=<file path> - file that contains piped lines that does not follow the schema'
    echo '   [-pipeEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8'
    echo '   [-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.SimilarPipes "$1" "$2" "$3" "$4" "$5" "$6"

if [ "$?" -ne 0 ]; then
  echo 'Similar pipes creation error'
  exit 1
fi

cd - || exit

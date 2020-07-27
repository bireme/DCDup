#!/bin/bash

JAVA_HOME=/usr/local/java11
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "4" ]
  then
    echo 'DocDuplicityExplain explain if a given input piped string representing a document is duplicated or not compared to '
    echo 'another one stored in a Lucene index (created from NGrams library) and represented by a document id.'
    echo
    echo 'Parameters:'
    echo '   -index=<indexPath> - Lucene index name/path'
    echo '   -schema=<schemaFile> - DeDup schema file'
    echo '   -id=<indexDocId> - id of the indexed document to compare'
    echo '   -doc=<pipeInputDocument> - input piped document to compare'
    echo '   [-schemaEncoding=<schemaFileEncoding>] - DeDup schema file encoding. Default is utf-8'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar  org.bireme.dcdup.DocDuplicityExplain $1 $2 $3 $4 $5 $6
#sbt "runMain org.bireme.dcdup.DocDuplicityExplain \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$6\""

cd - || exit

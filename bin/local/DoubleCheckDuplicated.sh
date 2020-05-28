#!/bin/bash

WHOAMI=`whoami`
if [ "$WHOAMI" != "operacao" ]
  then
    echo "You should execute this shell as 'operacao' user"
    exit 1
fi

JAVA_HOME=/usr/local/oracle-8-jdk
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "7" ]
  then
    echo 'DoubleCheckDuplicated shell takes a list of documents (defined by an schema'
    echo 'file) from a piped file and check then with a local DeDup index'
    echo "looking for duplicated files."
    echo
    echo 'usage: DoubleCheckDuplicated'

    echo '-pipe=<pipeFile> - DeDup piped input file'
    echo '-index=<indexPath> - DeDup index path'
    echo '-schema=<schemaFile> - DeDup schema file'
    echo '-outDupFile1=<outDupFile1> - duplicated records found in pipe file'
    echo '-outDupFile2=<outDupFile2> - duplicated records found between pipe file and DeDup index'
    echo '-outNoDupFile1=<outNoDupFile1> - no duplicated records between input pipe file and Dedup index'
    echo '-outNoDupFile2=<outNoDupFile2> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)'
    echo '[-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8'
    echo '[-pipeEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.DoubleCheckDuplicated $1 $2 $3 $4 $5 $6 $7 $8 $9
#sbt "runMain org.bireme.dcdup.DoubleCheckDuplicated $1 $2 $3 $4 $5 $6 $7 $8 $9"

cd - || exit


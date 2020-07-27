#!/bin/bash

JAVA_HOME=/usr/local/java11
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "8" ]
  then
    echo 'MySQL2Lucene shell takes a list of documents retrieved from a MySQL database and'
    echo 'creates a remote DeDup index with them. If such index already exists, it will rewritten.'
    echo
    echo 'usage: MySQL2Lucene'
    echo '   -host=<MySQL server> - IP or domain of the MySQL server'
    echo '   -user=<MySQL user> - MySQL user name'
    echo '   -pswd=<MySQL password> - MySQL user password'
    echo '   -dbnm=<MySQL database> - MySQL database name'
    echo '   -sqls=<MySQL sql file list> - comma separated sql file names'
    echo '   -dedupUrl=<DeDupBaseUrl> - DeDup url service  (for ex: http://dedup.bireme.org/services)'
    echo '   -index=<indexName> - DeDup index name'
    echo '   -schema=<schemaName> - DeDup schema name'
    echo
    echo '   [-port=<MySQL_Port>] - MySQL port'
    echo '   [-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8'
    echo '   [-jsonField=<tag>[,<tag>,...,<tag>]] - if a column element is a json element, indicates which json elements'
    echo '                                          to retrieve the content. Default are text,_f,_e.'
    echo '   [-repetitiveField=<name>[.<name>,...,<name>]] - the name of the fields that should be broken into a new line'
    echo '
                when the repetitive separator symbol is found. Default is title.'
    echo '   [-repetitiveSep=<separator>] - repetitive field string separator. Default is //@//'
    echo '   [-jsonLangField=<jsonLangField>] - the json field that store the language indicator. If present it will be'
    echo '                                      used to suffix the id field with the language.'
    echo '   [-idFieldName=<name>] - id field name of the mysql record]. Will be used to prefix the id with the language.'
    echo '                           if the jsonLangField is specified.'
    echo '   [--append] - append documents to an existing Lucene index.'

    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

NOW=$(date +"%Y%m%d%H%M%S")

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.MySQL2Pipe -pipe=pipeOut_$NOW.txt -jsonField=text,_f,_e -repetitiveField=title $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16}

if [ "$?" -ne 0 ]; then
  echo 'Pipe file generation error'
  exit 1
fi

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.WebPipe2Lucene -pipe=pipeOut_$NOW.txt -pipeEncoding=utf-8 $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16}

#rm pipeOut_$NOW.txt

cd - || exit


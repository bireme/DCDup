#!/bin/bash

JAVA_HOME=/usr/local/java11
PATH=$JAVA_HOME/bin:$PATH

if [ "$#" -lt "6" ]
  then
    echo 'MySQL2Pipe shell takes a list of documents retrieved from a MySQL database'
    echo 'and creates a local pipe file following a given schema file.'
    echo
    echo 'usage: MySQL2Pipe'
    echo '   -host=<MySQL server> - IP or domain of the MySQL server'
    echo '   -user=<MySQL user> - MySQL user name'
    echo '   -pswd=<MySQL password> - MySQL user password'
    echo '   -dbnm=<MySQL database> - MySQL database name'
    echo '   -sqls=<MySQL sql file list> - comma separated sql file names'
    echo '   -pipe=<pipeFile> - output piped file'
    echo
    echo '   [-port=<MySQL_Port>] - MySQL port'
    echo '   [-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8'
    echo '   [-pipeEncoding=<pipeEncoding>] - output piped file encoding. Default is utf-8'
    echo '   [-jsonField=<tag>[,<tag>,...,<tag>]] - if a column element is a json element, indicates'
    echo '                                          which json elements to retrieve the content'
    echo '   [-repetitiveField=<name>[.<name>,...,<name>]] - the name of the fields that should be broken into a new'
    echo '                                                   line when the repetitive separator symbol is found'
    echo '   [-repetitiveSep=<separator>] - repetitive field string separator. Default is //@//'
    echo '   [-jsonLangField=<jsonLangField>] - the json field that store the language indicator.'
    echo '                                      If present it will be used to suffix the id field with the language'
    echo '   [-idFieldName=<name>] - id field name of the mysql record]. Will be used to prefix the id with the language'
    echo '                           if the jsonLangField is specified'
    exit 1
fi

cd /home/javaapps/sbt-projects/DCDup || exit

java -cp target/scala-2.13/DCDup-assembly-0.1.0.jar org.bireme.dcdup.MySQL2Pipe "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" "${12}" "${13}" "${14}"

cd - || exit


#!/bin/bash

MX_HOME=/usr/local/bireme/cisis/5.7c/linux64/lindG4

if [ ! "$#" -eq "3" ]
  then
    echo "GeraPipedFile_Sas shell takes an Isis database (with Lilacs fields)"
    echo "and extracts all Sas (articles) records from it converting them into"
    echo "a piped file to be used as input by WebPipe2Lucene.sh shell."
    echo
    echo 'usage: GeraPipedFile_Sas'
    echo '   <lilacsLikeDb> - isis master files following Lilacs structure'
    echo '   <outputPipedFile> - output file used by DeDup as input file'
    echo '   <name database> '
    exit 1
fi

$MX_HOME/mx $1 "pft=if v5.1='S' then s3:=(v30^*),(if p(v12) then '$3_Sas|',v2[1],'|',replace(v12^*,'|',''),'|',s3,'|',v65[1].4,'|',v31[1],'|',v32[1],/ fi),(if p(v13) then '$3_Sas|',v2[1],'|',replace(v13^*,'|',''),'|',s3,'|',v65[1].4,'|',v31[1],'|',v32[1],/ fi),fi" lw=0 tell=50000 now > $2

#!/bin/bash

MX_HOME=/usr/local/bireme/cisis/5.7c/linux64/lindG4

if [ ! "$#" -eq "2" ]
  then
    echo "GeraPipedFile_Sas shell takes an Isis database (with Lilacs fields)"
    echo "and extracts all Sas (articles) records from it converting them into"
    echo "a piped file to be used as input by WebPipe2Lucene.sh shell."
    echo
    echo 'usage: GeraPipedFile_Sas'
    echo '   <lilacsLikeDb> - isis master files following Lilacs structure'
    echo '   <outputPipedFile> - output file used by DeDup as input file'
    exit 1
fi

$MX_HOME/mx $1 "pft=if v5.1='S' then s1:=(if p(v10) then (|//@//|+v10^*) else (|//@//|+v11^*) fi), s3:=(v30^*),(if p(v12) then 'LILACS_Sas|',v902[1],'|',replace(v12^*,'|',''),'|',s3,'|',v65[1].4,'|',v31[1],'|',v32[1]'|',s1,'|',if v14[1]^*>'' then v14[1]^* fi,/ fi),(if p(v13) then 'LILACS_Sas|',v902[1],'|',replace(v13^*,'|',''),'|',s3,'|',v65[1].4,'|',v31[1],'|',v32[1]'|',s1,'|',if v14[1]^*>'' then v14[1]^* fi,/ fi),fi" lw=0 tell=50000 now > $2

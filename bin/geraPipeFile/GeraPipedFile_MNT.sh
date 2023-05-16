#!/bin/bash

MX_HOME=/usr/local/bireme/cisis/5.7c/linux64/lindG4

if [ ! "$#" -eq "3" ]
  then
    echo "GeraPipedFile_MNT shell takes an Isis database (with Lilacs fields)"
    echo "and extracts all MNT (monographic, thesi and non conventional) records from it converting them into"
    echo "a piped file to be used as input by WebPipe2Lucene.sh shell."
    echo
    echo 'usage: GeraPipedFile_MNT'
    echo '   <lilacsLikeDb> - isis master files following Lilacs structure'
    echo '   <outputPipedFile> - output file used by DeDup as input file'
    echo '   <name database> '
    exit 1
fi

$MX_HOME/mx $1 "pft=if not v5.1='S' and v6.1='m' then s1:=(if p(v16) then (|//@//|+v16^*) else (|//@//|+v17^*) fi), (if p(v18) then '$3_MNT|',v2[1],'|',replace(v18^*,'|',''),'|',v65[1].4,'|',s1,'|',if v20[1]^*>'' then v20[1]^* fi,/ fi),(if p(v19) then '$3_MNT|',v2[1],'|',replace(v19^*,'|',''),'|',v65[1].4,'|',s1,'|',if v20[1]^*>'' then v20[1]^* fi,/ fi),fi" lw=0 tell=50000 now > $2

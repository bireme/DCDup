#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup || exit

CheckFileName=`date '+%Y%m%d'.chk`
sbt test &> ./$CheckFileName

Errors=`grep -c "*** FAILED ***" ./$CheckFileName`
if [ "$Errors" != "0" ]; then
  sendemail -f appofi@bireme.org -u "DeDup Service - Check ERROR - `date '+%Y%m%d'`" -m "DeDup Service - Check ERROR" -a $CheckFileName -t barbieri@paho.org -cc antoniov@paho.org;ofi@bireme.org -s esmeralda.bireme.br -xu serverofi -xp bir@2012#
  rm ./$CheckFileName
  exit 1
fi

rm ./$CheckFileName

cd - || exit


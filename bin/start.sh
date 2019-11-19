#!/usr/bin/bash

if [ $# -lt 1 ];then
  echo "Usage: start.sh weikadai"
  exit
fi
job=$1
script="submit_${job}.sh"
log="running_${job}.log"
echo "submiting, log:${log}, script:${script}"
cat logs/${log} >> logs/${log}.old
nohup ./${script} > logs/${log} 2>&1 &
echo "submit done"
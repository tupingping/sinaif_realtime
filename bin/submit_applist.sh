#!/usr/bin/bash

# --jars *.jar
spark-submit --master local[*] \
    --name AppListRealtimeJob \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --queue default \
    --class com.sinaif.realtime.AppListRealtimeJob \
    jars/sinaif_realtime-1.2.ja
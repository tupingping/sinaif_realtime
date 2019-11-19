#!/usr/bin/bash

# --jars *.jar
spark-submit --master local[*] \
    --name SensorEventRealtimeJob \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --queue default \
    --class com.sinaif.realtime.SensorEventRealtimeJob \
    jars/sinaif_realtime-1.0.jar
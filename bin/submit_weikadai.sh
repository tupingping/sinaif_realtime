#!/usr/bin/bash

# --jars *.jar
# cluster 模式会报错：`java.sql.SQLException: No suitable driver found`，暂时未解决，先用client模式
# 已使用cluster
# https://dongkelun.com/2018/05/06/sparkSubmitException/
spark-submit --master yarn \
    --deploy-mode cluster \
    --name WeiKaDaiRealtimeJob \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.executor.extraClassPath=jars/mysql-connector-java-5.1.45.jar \
    --conf spark.driver.extraClassPath=jars/mysql-connector-java-5.1.45.jar \
    --jars jars/mysql-connector-java-5.1.45.jar \
    --driver-class-path jars/mysql-connector-java-5.1.45.jar \
    --queue default \
    --class com.sinaif.realtime.WeiKaDaiRealtimeJob \
    jars/sinaif_realtime-1.0.jar
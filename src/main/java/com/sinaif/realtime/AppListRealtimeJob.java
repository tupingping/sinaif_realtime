package com.sinaif.realtime;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mongodb.client.model.Filters;
import com.sinaif.utils.AppListUtils;
import com.sinaif.utils.DateUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.conversions.Bson;

import java.util.*;

import static com.mongodb.client.model.Updates.*;
import static com.sinaif.utils.StringUtil.jsonParser;

/**
 * @Time : 2019/10/9 14:19
 * @Author : pingping.tu
 * @File : AppListRealtimeJob.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class AppListRealtimeJob {
    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf()
                .setAppName(AppListUtils.props.getProperty("spark.app.name","AppListRealtimeJob"))
                .setMaster(AppListUtils.props.getProperty("spark.master","yarn"))
                ;

        sparkConf.set("spark.io.compression.codec", AppListUtils.props.getProperty("spark.io.compression.codec", "lz4"));
        sparkConf.set("spark.eventLog.enabled",AppListUtils.props.getProperty("spark.eventLog.enabled", "false"));

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,
                Durations.seconds(Integer.parseInt(AppListUtils.props.getProperty("spark.durations.seconds","10"))));

        jsc.sparkContext().setLogLevel(AppListUtils.props.getProperty("spark.log.level","WARN"));

        Set<String> topicSet = new HashSet<>(Arrays.asList(AppListUtils.props.getProperty("kafka.topics").split(",")));

        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppListUtils.props.getProperty("kafka.bootstrap.servers"));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, AppListUtils.props.getProperty("kafka.group.id"));
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppListUtils.props.getProperty("kafka.auto.offset.reset"));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicSet, kafkaParams));

        JavaDStream<String> lines = messages.map(x->x.value());

        lines.foreachRDD(rdd ->{
            rdd.foreachPartition(partition->{
                partition.forEachRemaining(raw->processMsg(raw));
            });
        });

        lines.count().print();
        jsc.start();
        jsc.awaitTermination();
    }


    public static void processMsg(String raw){

        try{
            JsonObject rawObj = jsonParser.parse(raw).getAsJsonObject();
            String request_body = rawObj.has("request_body") ? rawObj.get("request_body").getAsString() : null;

            JsonObject body = jsonParser.parse(request_body).getAsJsonObject();
            String content = body.has("content") ? body.get("content").getAsString() : null;

            if(content != null && content.contains("appname")){
                int appnum = 0;
                JsonArray applist = jsonParser.parse(content).getAsJsonArray();

                for(int i = 0; i < applist.size(); i++){
                    JsonObject jo = applist.get(i).getAsJsonObject();
                    if(jo.has("appname")){
                        String name = jo.get("appname").getAsString();
                        if(name.contains("融") || name.contains("贷") || name.contains("花") ||
                                name.contains("钱") || name.contains("金") || name.contains("卡") ||
                                name.contains("信用") || name.contains("财富")){
                            appnum ++;
                        }
                    }
                }

                String productid = body.get("productid").getAsString();
                String useid = body.get("userid").getAsString();
                String uptime = DateUtil.currentDate("yyyy-MM-dd HH:mm:ss");

                Bson filter = Filters.and(Filters.eq("productid", productid), Filters.eq("userid", useid));
                Bson update = combine(setOnInsert("crtime",uptime), set("uptime", uptime), set("phonecollect.appname", appnum));

                AppListUtils.upsert(raw, filter, update);

            }

        }catch (Exception e){
            System.out.println(raw);
            e.printStackTrace();
        }

    }

}

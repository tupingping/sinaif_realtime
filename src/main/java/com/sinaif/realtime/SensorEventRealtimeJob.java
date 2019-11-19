package com.sinaif.realtime;

import static com.mongodb.client.model.Updates.*;
import static com.sinaif.utils.SensorUtils.*;
import static com.sinaif.utils.StringUtil.jsonParser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import com.google.gson.JsonObject;
import com.mongodb.client.model.Filters;
import com.sinaif.utils.DateUtil;
import com.sinaif.utils.StringUtil;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <groupId> <topics> <brokers> is a list of
 * one or more Kafka brokers <groupId> is a consumer group name to consume from
 * topics <topics> is a list of one or more kafka topics to consume from
 * <p>
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ consumer-group topic1,topic2
 */

public final class SensorEventRealtimeJob {

    public static void main(String[] args) throws Exception {
        // System.setProperty("hadoop.home.dir", "D:\\DevInstall\\hadoop-2.6.5");
        // Driver driver = new com.mysql.jdbc.Driver();
        // DBClient.init();
        // Launch quartz Jobs（flush2DBJob，mergeUidJob等）
        SparkConf sparkConf = new SparkConf()
                .setAppName(props.getProperty("spark.app.name", "SensorEventRealtimeJob"))
				.setMaster(props.getProperty("spark.master", "yarn"))
                ;
        sparkConf.set("spark.io.compression.codec", props.getProperty("spark.io.compression.codec", "lz4"));
        sparkConf.set("spark.eventLog.enabled", props.getProperty("spark.eventLog.enabled", "false"));

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(Integer.parseInt(props.getProperty("spark.durations.seconds", "10"))));
        jssc.sparkContext().setLogLevel(props.getProperty("spark.log.level", "WARN"));

        Set<String> topicsSet = new HashSet<String>(Arrays.asList(props.getProperty("kafka.topics").split(",")));
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("kafka.bootstrap.servers"));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty("kafka.group.id"));
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.getProperty("kafka.auto.offset.reset"));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, parse Json String
        JavaDStream<String> lines = messages.map(x -> x.value());

        lines.foreachRDD(line -> {
//            OffsetRange[] offsetRanges = ((HasOffsetRanges)line.rdd()).offsetRanges();
            line.foreachPartition(eachPartition -> {
                eachPartition.forEachRemaining(raw -> processMsg(raw));
//                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });
        lines.count().print();
        // System.out.println("count：" + lines.count().count());
        jssc.start();
        jssc.awaitTermination();
    }

    public static void processMsg(String raw) {
        /**
         * kafka数据流非标准json格式，数据里面乱码/url编码混乱
         */
        // .replace("\\\"", "\"").replace("\\\\", "").replace("\\{", "{").replace("}\"",
        // "}").replace("\\[", "[").replace("]\"", "]");
        // System.out.println(x);
        String distinctId = "";
        String event = "";
        String project = "";
        Long time;
        try {
            JsonObject rawObj = jsonParser.parse(raw).getAsJsonObject();
            event = rawObj.has("event") ? rawObj.get("event").getAsString() : null;
            distinctId = rawObj.has("distinct_id") ? rawObj.get("distinct_id").getAsString() : null;
            project = rawObj.has("project") ? rawObj.get("project").getAsString() : null;
            time = Long.parseLong(rawObj.get("time").getAsString());
            if(event == null || distinctId == null || project == null){
                return;
            }
            String mongoProductid = projectMaps.get(project);
            String mongoStatField = eventMaps.get(event);
            if ((!StringUtil.isEmpty(mongoProductid)) && (!StringUtil.isEmpty(mongoStatField))) {
                if (time/1000 < calStartTime) {
                    // 初始化完成之前的消息，直接跳过
                    return;
                }
                String uptime = DateUtil.currentDate("yyyy-MM-dd HH:mm:ss");
                Bson filter = Filters.and(Filters.eq("productid", mongoProductid), Filters.eq("userid", distinctId));
                Bson update = combine(setOnInsert("crtime", uptime),
                        inc("click_event_stat." + mongoStatField, 1),
                        set("click_event_stat.uptime", uptime),
                        set("uptime", uptime));
                upsertWithRetry(raw, filter, update);
            }
        } catch (Exception e) {
            System.out.println(raw);
            e.printStackTrace();
        }
    }
}

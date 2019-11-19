package com.sinaif.realtime;

import static com.sinaif.utils.StringUtil.jsonParser;
import static com.sinaif.utils.WeiKaDaiUtils.props;
import static com.sinaif.utils.WeiKaDaiUtils.redis;

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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sinaif.utils.RedisUtil;
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

public final class WeiKaDaiRealtimeJob {

	public static void main(String[] args) throws Exception {
		// System.setProperty("hadoop.home.dir", "D:\\DevInstall\\hadoop-2.6.5");
		// Driver driver = new com.mysql.jdbc.Driver();
        // DBClient.init();
		// Launch quartz Jobs（flush2DBJob，mergeUidJob等）

		SparkConf sparkConf = new SparkConf()
				.setAppName(props.getProperty("spark.app.name", "WeiKaDaiRealtimeJob"))
				.setMaster(props.getProperty("spark.master", "yarn"));
		sparkConf.set("spark.io.compression.codec", props.getProperty("spark.io.compression.codec", "lz4"));
		sparkConf.set("spark.eventLog.enabled", props.getProperty("spark.eventLog.enabled", "false"));

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(Integer.parseInt(props.getProperty("durations.seconds", "10"))));
		jssc.sparkContext().setLogLevel(props.getProperty("log.level", "WARN"));

		Set<String> topicsSet = new HashSet<String>(Arrays.asList(props.getProperty("topics").split(",")));
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty("group.id"));
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.getProperty("auto.offset.reset"));
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		// Get the lines, parse Json String
		JavaDStream<String> lines = messages.map(x -> x.value());

		lines.foreachRDD(line -> {
			line.foreachPartition(eachPartition -> {
				eachPartition.forEachRemaining(raw -> processMsg(raw));
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
		String dataStr = "";
		JsonArray dataArray = null;
		try {
			dataStr = jsonParser.parse(raw).getAsJsonObject().get("request_body").getAsString();

			dataArray = jsonParser.parse(dataStr).getAsJsonObject().get("data").getAsJsonArray();
			if (dataArray == null) {
				return;
			}
			for (JsonElement element : dataArray) {
				JsonObject msgJson = element.getAsJsonObject();
				String productId = StringUtil.getJsonElementStr(msgJson, "productId");
				String channel = StringUtil.getJsonElementStr(msgJson, "channel");
				String currEvent = StringUtil.getJsonElementStr(msgJson, "currEvent");
				// String createTime = clickJson.get("createTime").getAsString();

				/**
				 * <pre/>
				 * 1、离线计算中 取visitStartTime最早的channel作为最终channel，实时计算中用同样算法计算成本偏高，
				 *    由于kafka消费基本是按照时间先后顺序的，可以近似的把处理的第一条消息的channel作为最终channel 
				 * 2、pv按消息数来计算
				 * 3、uv按uid去重的消息数来计算，newUv需要离线表过滤，这里暂时不计算
				 * 4、newUV数据如果需要计算，需要将离线数据每天刷新到redis
				 * 5、按visitStartTime作为时间标准 
				 * 6、只统计productid=2005 and currEvent in ('W_start_001','WH_ST_612')
				 * 
				 * 输出统计结果：dt，channel，pv，uv，newUV
				 * 
				 */
				if (channel != null && "2005".equals(productId)
						&& ("W_start_001".equals(currEvent) || "WH_ST_612".equals(currEvent))) {
					JsonElement dedicated = StringUtil.jsonParser.parse(msgJson.get("dedicated").getAsString());
					if (!dedicated.isJsonObject()) {
						return;
					}
					String uid = StringUtil.getJsonElementStr(dedicated.getAsJsonObject(), "weibouid");
					if (StringUtil.isEmpty(uid)) {
						return;
					}
					String visitStartTime = StringUtil.getJsonElementStr(msgJson, "visitStartTime");
					// Date date = StringUtil.yyyy_MM_ddHHmmss2Date(visitStartTime);
					String curDate = visitStartTime.replace("-", "").substring(0, 8);
					// Redis Set {channel}
					String channelDictKey = RedisUtil.channelDictKey(curDate);
					// Redis Set {uid}，当天同一个channel包含的uid集合（用于去重判断）
					String channel2UidsKey = RedisUtil.channel2UidsKey(curDate, channel);
					// Redis Hash {pv}:{num},{uv}:{num}，一天之内同一个channel的指标数据（pv，uv，newuv）
					String channelStatKey = RedisUtil.channelStatKey(curDate, channel);
					// Redis Hash {uid}:{channel}，uid绑定的channel（第一条日志的channel）
					String uid2ChannelKey = RedisUtil.uid2ChannelKey(curDate);
					// Redis Set {uid}，今天之前的所有uid集合（用于判断newUV）
					String pastUidKey = RedisUtil.pastUidKey();

					// maintain channels
					redis.sadd(channelDictKey, channel, RedisUtil._EXPIRE_ONE_WEEK);

					// pv + 1
					redis.hincrBy(channelStatKey, "pv", 1, RedisUtil._EXPIRE_ONE_WEEK);

					// 1、检查当前uid是否已经绑定到某个channel，如果没有就设置，如果已经绑定，检查绑定channel是否与当前一致
					// 2、同时检查当前channel的uid集合中是否存在当前uid
					if ((redis.hsetnx(uid2ChannelKey, uid, channel) == 1
							|| channel.equals(redis.hget(uid2ChannelKey, uid)))
							&& redis.sadd(channel2UidsKey, uid, RedisUtil._EXPIRE_ONE_WEEK) > 0) {
						// uv + 1
						redis.hincrBy(channelStatKey, "uv", 1, RedisUtil._EXPIRE_ONE_WEEK);
						if (!redis.sismember(pastUidKey, uid)) {
							// newuv + 1
							redis.hincrBy(channelStatKey, "newuv", 1, RedisUtil._EXPIRE_ONE_WEEK);
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println(raw);
			e.printStackTrace();
		}

	}
}

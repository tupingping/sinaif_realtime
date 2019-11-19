package com.sinaif.utils;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCluster;

public class RedisClusterClient implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
	        .getLogger(RedisClusterClient.class);

	/** Redis集群 */
	private static JedisCluster jedisCluster;

	/** 记录累计执行的命令和总耗时<commoand,cost>，一般作调试用 */
	private ConcurrentHashMap<String, AtomicLong> costMap = new ConcurrentHashMap<String, AtomicLong>();
	/** 记录累计执行的命令和总次数<commoand,times>，一般作调试用 */
	private ConcurrentHashMap<String, AtomicLong> timesMap = new ConcurrentHashMap<String, AtomicLong>();

	private RedisClusterClient() {
	};

	static {
		jedisCluster = RedisConnectionUtils.getJedisCluster();
	}

	public static JedisCluster getInstance() {
		return jedisCluster;
	}

	public void record(String command, long milliseconds) {
		AtomicLong times = timesMap.get(command);
		AtomicLong cost = costMap.get(command);
		if (null == times) {
			times = new AtomicLong(0);
		}
		if (null == cost) {
			cost = new AtomicLong(0);
		}
		long totalTimes = times.incrementAndGet();
		long totalCost = cost.addAndGet(milliseconds);
		timesMap.put(command, times);
		costMap.put(command, cost);
		if (totalTimes % 10000 == 0) { // 每10000个命令统计一次总耗时
			LOG.info(">>>>>>>>>>redis stats, command:" + command
			        + ", 10000 commands cost: " + totalCost + " ms");
			times.set(0);
			cost.set(0);
		}
	}
}

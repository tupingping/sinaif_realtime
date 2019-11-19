package com.sinaif.utils;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RedisExpireHelper {

	private static final Logger LOG = LoggerFactory
	        .getLogger(RedisExpireHelper.class);
	private static ConcurrentHashMap<String, Integer> expireClusterMap = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentHashMap<String, Integer> expireMap = new ConcurrentHashMap<String, Integer>();

	static {
		new RedisKeyExpirer().start();
	}

	public static void expireClusterKey(String key, int ttl) {
		expireClusterMap.put(key, ttl);
	}

	// 处理集群 | 单机模式下的过期key
	public static void expireKey(String key, int ttl) {
		expireMap.put(key, ttl);
	}

	static class RedisKeyExpirer extends Thread {
		RedisClient redisClient = RedisClient.getInstance();
		Set<String> toDelKeys = new HashSet<String>();

		public void run() {
			while (true) {
				try {
					int size = expireMap.size();
					LOG.info("expireMap size-->" + size);
					long sleepTime = getSleepTime(size);
					// expire all the keys in expireClusterMap
					for (Entry<String, Integer> entry : expireMap.entrySet()) {
						redisClient.expire(entry.getKey(), entry.getValue());
						toDelKeys.add(entry.getKey());
					}
					// clear the keys which had been expired in expireClusterMap
					for (String expiredKeys : toDelKeys) {
						expireMap.remove(expiredKeys);
					}
					// clear toDelKeys
					toDelKeys.clear();

					LOG.info("Finish expire all the keys in expireMap, sleep for "
					        + sleepTime + "s");
					Thread.sleep(sleepTime * 1000);
				} catch (Throwable t) {
					LOG.error("expireing key faileds", t);
				}
			}
		}
	};

	public static int getSleepTime(int mapSize) {
		if (mapSize < 1000)
			return 5 * 60; // sleep for 10m
		if (mapSize < 10000)
			return 2 * 60; // sleep for 5m
		return 1 * 60; // sleep for 1m
	}

}

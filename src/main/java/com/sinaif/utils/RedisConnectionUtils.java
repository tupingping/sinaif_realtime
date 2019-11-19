package com.sinaif.utils;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionUtils {

	private static final Logger LOG = LoggerFactory
	        .getLogger(RedisConnectionUtils.class);
	private static JedisPoolConfig jedisPoolConfig;
	private static final int maxTotal = 2000;
	private static final int maxIdle = 100;
	private static final int maxWaitMillis = 2000;
	private static Properties props;

	// 默认单点
	private static final String defaultNode = "192.168.1.1:6379";

	// 默认集群节点
	private static final String defaultClusterNodes = "192.168.1.1:7000,192.168.1.2:7000,192.168.1.180:7000,192.168.1.3:7000";

	private static JedisCluster jedisCluster;
	private static JedisPool jedisPool;

	static {
		props = new Properties();
		try {
			props.load((RedisConnectionUtils.class
			        .getClassLoader().getResourceAsStream("redis.properties")));
		} catch (Exception e) {
			LOG.error("can't find redis.properties in classpath, use default");
		}

		// 初始化连接池配置
		initJedisPoolConfig();
	}

	private static void initJedisPoolConfig() {
		LOG.info("init jedisPoolConfig...");
		jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(getIntProperty("redis.pool.maxTotal",
		        maxTotal));
		jedisPoolConfig
		        .setMaxIdle(getIntProperty("redis.pool.maxIdle", maxIdle));
		jedisPoolConfig.setMaxWaitMillis(getIntProperty(
		        "redis.pool.maxWaitMillis", maxWaitMillis));
		jedisPoolConfig.setTestOnBorrow(true);
		jedisPoolConfig.setTestOnReturn(true);
		LOG.info("maxTotal:" + jedisPoolConfig.getMaxTotal() + ", maxIdle:"
		        + jedisPoolConfig.getMaxIdle() + ", maxWaitMillis:"
		        + jedisPoolConfig.getMaxWaitMillis());
	}

	public synchronized static void initJedisPool() {
		if (null == jedisPool) {
			String hostAndPort = props.getProperty("redis.standalone.node",
			        defaultNode);
			String host = hostAndPort.split(":")[0];
			int port = Integer.parseInt(hostAndPort.split(":")[1]);
			try {
				jedisPool = new JedisPool(jedisPoolConfig, host, port);
				Jedis jedis = jedisPool.getResource();
				jedis.set("shawshank", "get busy living or get busy dying");
				LOG.info("Redis standalone echo: " + jedis.get("shawshank"));
			} catch (Exception e) {
				LOG.error("Redis standalone doesn't response ,Please check !");
				e.printStackTrace();
			}
		}
	}

	public synchronized static void initJedisCluster() {
		if (null == jedisCluster) {
			Set<HostAndPort> nodes = getHostAndPortProperty(
			        "redis.cluster.nodes", defaultClusterNodes);
			LOG.info("Redis cluster nodes: " + nodes);
			try {
				jedisCluster = new JedisCluster(nodes, jedisPoolConfig);
				jedisCluster.set("yoda", "May the force be with you!");
				LOG.info("Redis cluster echo: " + jedisCluster.get("yoda"));
			} catch (Exception e) {
				LOG.error("Redis cluster doesn't response ,Please check !");
				e.printStackTrace();
			}
		}
	}

	private static int getIntProperty(String key, int defaultValue) {
		int val = defaultValue;
		try {
			if (!isEmpty(props.getProperty(key))) {
				val = Integer.parseInt(props.getProperty(key));
			}
		} catch (Exception e) {
			LOG.info("property is not exists or invalid ,use default[" + key
			        + ":" + defaultValue + "]");
		}
		return val;
	}

	private static Set<HostAndPort> getHostAndPortProperty(String key,
	        String defaultValue) {
		// 格式�?192.168.1.80:8000,192.168.1.81:8001
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		String hostAndPort = props.getProperty(key);
		hostAndPort = isEmpty(hostAndPort) ? defaultValue : hostAndPort;
		String[] hostAndPorts = hostAndPort.split(",");
		for (String node : hostAndPorts) {
			String host = node.split(":")[0];
			int port = Integer.parseInt(node.split(":")[1]);
			nodes.add(new HostAndPort(host, port));
		}
		return nodes;
	}

	public static JedisPool getJedisPool() {
		initJedisPool();
		return jedisPool;
	}

	public static JedisCluster getJedisCluster() {
		initJedisCluster();
		return jedisCluster;
	}

	private static boolean isEmpty(String value) {
		if (null == value || value.trim().equals("")) {
			return true;
		}
		return false;
	}

}

package com.sinaif.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisClient implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
	        .getLogger(RedisClient.class);

	private static JedisPool jedisPool;

	private static RedisClient redisClient;

	private RedisClient() {
	};

	static {
		jedisPool = RedisConnectionUtils.getJedisPool();
		redisClient = new RedisClient();
	}

	/**
	 * <pre>
	 * @see RedisClusterClient#getInstance()
	 */
	public static RedisClient getInstance() {
		return redisClient;
	}

	public String get(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return jedis.get(key);
		} catch (Exception e) {
			LOG.error("get error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long setnx(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			long result = jedis.setnx(key, value);
			return result;
		} catch (Exception e) {
			LOG.error("setnx error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Long setnx(String key, String value, int seconds) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			long result = jedis.setnx(key, value);
			if (result == 1) {
				jedis.expire(key, seconds);
			}
			LOG.debug(key + "-" + result);
			return result;
		} catch (Exception e) {
			LOG.error("setnx error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Boolean set(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("Set the string value as value of the key, Key=" + key);
			jedis.set(key, value);
			return true;
		} catch (Exception e) {
			LOG.error("set error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return false;
	}

	public Boolean set(String key, String value, int seconds) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("Set the string value as value of the key, Key=" + key);
			jedis.set(key, value);
			jedis.expire(key, seconds);
			return true;
		} catch (Exception e) {
			LOG.error("set error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return false;
	}

	/**
	 * 删除指定key
	 * 
	 * @param key
	 * @return
	 */
	public Long del(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("remove the specified keys :" + key);
			return jedis.del(key);
		} catch (Exception e) {
			LOG.error("del error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Boolean lpush(String key, String... strings) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("add the string value to the head (LPUSH) of the list stored at key. key="
			        + key);
			for (String string : strings) {
				jedis.lpush(key, string);
			}
			return true;
		} catch (Exception e) {
			LOG.error("lpush error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return false;
	}

	public Boolean rpush(String key, String... strings) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("add the string value to the tail (RPUSH) of the list stored at key. key="
			        + key);
			for (String string : strings) {
				jedis.rpush(key, string);
			}
			return true;
		} catch (Exception e) {
			LOG.error("rpush error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return false;
	}

	/**
	 * 返回list中指定范围的元素
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<String> lrange(String key, long start, long end) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("return the specified elements of the list stored at the specified key. key="
			        + key);
			return jedis.lrange(key, start, end);
		} catch (Exception e) {
			LOG.error("lrange error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * 返回list长度
	 * 
	 * @param key
	 * @return
	 */
	public Long llen(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("return the length of the list stored at the specified key. key="
			        + key);
			return jedis.llen(key);
		} catch (Exception e) {
			LOG.error("llen error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * 向指定key的set集合中加入新成员 如果是新元素返回1，元素在集合中已存在返回0.
	 * 
	 * @param key
	 * @param members
	 * @param seconds
	 * @return
	 */
	public long sadd(String key, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("add the specified member to the set value stored at key. key="
			        + key);
			return jedis.sadd(key, member);
		} catch (Exception e) {
			LOG.error("sadd error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0;
	}

	public long sadd(String key, String member, int ttl) {
		long result = 0;
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("add the specified member to the set value stored at key. key="
			        + key);
			result = jedis.sadd(key, member);
			if (result > 0) {
				jedis.expire(key, ttl);
			}
		} catch (Exception e) {
			LOG.error("sadd error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}

		return result;
	}

	public long sadd(String key, String member, int ttl, boolean asyncExpire) {
		long result = 0;
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("add the specified member to the set value stored at key. key="
			        + key);
			result = jedis.sadd(key, member);
			if (result > 0) { // 设置 key 的过期时�?
				if (asyncExpire) { // 异步设置过期
					RedisExpireHelper.expireKey(key, ttl); // 设置为非集群
				} else {
					jedis.expire(key, ttl);
				}
			}
		} catch (Exception e) {
			LOG.error("sadd error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}

		return result;
	}

	/**
	 * 判断某个成员是否属于指定key的set集合
	 * 
	 * @see #sismember(String, String, boolean)
	 * @param
	 * @return boolean
	 */
	public Boolean sismember(String key, String member) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("if member is a member of the set stored at key. key="
			        + key);
			return jedis.sismember(key, member);
		} catch (Exception e) {
			LOG.error("sismenber error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * 返回指定key的set集合
	 * 
	 * @param
	 * @return boolean
	 */
	public Set<String> smembers(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("return all the members (elements) of the set value stored at key. key="
			        + key);
			return jedis.smembers(key);
		} catch (Exception e) {
			LOG.error("smembers error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long srem(String key, String... members) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("remove the specified member from the set value stored at key. key="
			        + key);
			for (String member : members) {
				jedis.srem(key, member);
			}
		} catch (Exception e) {
			LOG.error("srem error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long scard(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("return the set cardinality (number of elements). key="
			        + key);
			return jedis.scard(key);
		} catch (Exception e) {
			LOG.error("scard error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}
	
	/**
	 * return 0 if an old value exists, or 1
	 * @param key
	 * @param field
	 * @param value
	 * @return 0 
	 */
	public Long hset(String key, String field, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("set the specified hash field to the specified value. key="
			        + key);
			return jedis.hset(key, field, value);
			
		} catch (Exception e) {
			LOG.error("hset error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}
	
	public Long hsetnx(String key, String field, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("set the specified hash field to the specified value. key="
			        + key);
			return jedis.hsetnx(key, field, value);
			
		} catch (Exception e) {
			LOG.error("hset error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}
	
	public String hget(String key, String field) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("retrieve the value associated to the specified field. key="
			        + key);
			return jedis.hget(key, field);
		} catch (Exception e) {
			LOG.error("hget error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long hdel(String key, String field) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("remove the specified field from an hash stored at key. key="
			        + key);
			return jedis.hdel(key, field);
		} catch (Exception e) {
			LOG.error("hdel error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long hlen(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("the number of items in a hash. key" + key);
			return jedis.hlen(key);
		} catch (Exception e) {
			LOG.error("hlen error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return (long) 0;
	}

	public Map<String, String> hgetAll(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("all the fields and associated values in a hash. key="
			        + key);
			return jedis.hgetAll(key);
		} catch (Exception e) {
			LOG.error("hgetAll error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * 从指定Key的hash中获取多个fields对应的value�?
	 * 
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> hmget(String key, String... fields) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("retrieve the values associated to the specified fields. key="
			        + key);
			return jedis.hmget(key, fields);
		} catch (Exception e) {
			LOG.error("hmget error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * 向指定Key的hash中设置多个fields-value�?
	 * 
	 * @param key
	 * @param hash
	 * @return
	 */
	public String hmset(String key, Map<String, String> hash) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("retrieve the values associated to the specified fields. key="
			        + key);
			return jedis.hmset(key, hash);
		} catch (Exception e) {
			LOG.error("hmget error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	/**
	 * @see #hincrBy(String, String, int)
	 * @param key
	 * @param hash
	 * @return
	 */
	@Deprecated
	public Long hincyBy(String key, String field, int value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("Increment the number stored at field in the hash at key by value. key="
			        + key);
			return jedis.hincrBy(key, field, value);
		} catch (Exception e) {
			LOG.error("hincrBy error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	/**
	 * 对指定Key的hash结构中的field对应的value作增加操�?
	 * 
	 * @param key
	 * @param hash
	 * @return
	 */
	public Long hincrBy(String key, String field, int value) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("Increment the number stored at field in the hash at key by value. key="
			        + key);
			return jedis.hincrBy(key, field, value);
		} catch (Exception e) {
			LOG.error("hincrBy error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Long hincrBy(String key, String field, int value, int ttl) {
		return hincrBy(key, field, value, ttl, false);
	}

	public Long hincrBy(String key, String field, int value, int ttl,
	        boolean asyncExpire) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("Increment the number stored at field in the hash at key by value. key="
			        + key);
			long count = jedis.hincrBy(key, field, value);
			if (asyncExpire) { // 异步设置过期
				RedisExpireHelper.expireKey(key, ttl);// 设置为非集群
			} else {
				expire(key, ttl);
			}
			return count;
		} catch (Exception e) {
			LOG.error("hincrBy error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Long incr(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			LOG.debug("increment the number stored at key by one. key=" + key);
			return jedis.incr(key);
		} catch (Exception e) {
			LOG.error("incr error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Long incrby(String key, long increment) {
		Jedis jedis = jedisPool.getResource();
		try {
			LOG.debug("increment the number stored at key by increment. key="
			        + key);
			return jedis.incrBy(key, increment);
		} catch (Exception e) {
			LOG.error("incrBy error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return 0L;
	}

	public Long expire(String key, int seconds) {
		Jedis jedis = jedisPool.getResource();
		try {
			LOG.debug("set a timeout on the specified key. key=" + key);
			return jedis.expire(key, seconds);
		} catch (Exception e) {
			LOG.error("expire error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}

	public Long expireAt(String key, long unixTime) {
		Jedis jedis = jedisPool.getResource();
		try {
			LOG.debug("set a timeout on the specified key. key=" + key);
			return jedis.expireAt(key, unixTime);
		} catch (Exception e) {
			LOG.error("expire error key=" + key, e);
		} finally {
			if (null != jedis) {
				jedis.close();
			}
		}
		return null;
	}
}

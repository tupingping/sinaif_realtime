package com.sinaif.utils;

public class RedisUtil {

	/**
	 * <pre/>
	 * RedisKey构造规则
	 * string 	{biz}:{key}:{other}
	 * set		{biz}:{key}:{other}
	 * zset		{biz}:{key}:{other}
	 * hash		{biz}:{key}:{other}
	 * 
	 * 通用字段说明
	 * info：表示hash里面的kv
	 * stat：表示hash里面的kv，多用于统计指标:统计值
	 * dict: 表示字典集合
	 */

	private final static String _KEY_PREFIX_WEIKADAI_CHANNEL_STAT = "wkd:ch:stat";
	private final static String _KEY_PREFIX_WEIKADAI_CHANNEL_UID = "wkd:ch:uid";
	private final static String _KEY_PREFIX_WEIKADAI_UID_PAST = "wkd:uid_past";
	private final static String _KEY_PREFIX_WEIKADAI_CHANNEL_DICT = "wkd:ch:dict";
	private final static String _KEY_PREFIX_WEIKADAI_UID_CHANNEL = "wkd:uid:ch";

	public final static int _EXPIRE_ONE_HOUR = 60 * 60;
	public final static int _EXPIRE_ONE_DAY = 24 * _EXPIRE_ONE_HOUR;
	public final static int _EXPIRE_ONE_WEEK = 7 * _EXPIRE_ONE_DAY;

	/**
	 * <pre/>
	 * Type：Set
	 * Value：指定日期的所有channel集合
	 * 		value
	 * 		{channel}
	 * @param date
	 * @return
	 */
	public static String channelDictKey(String date) {
		return RedisUtil._KEY_PREFIX_WEIKADAI_CHANNEL_DICT + ":" + date;
	}

	/**
	 * <pre/>
	 * Type：Set
	 * Value：指定日期和channel的所有uid集合
	 * 		value
	 * 		{uid}
	 * @param date
	 * @return
	 */
	public static String channel2UidsKey(String date, String channel) {
		return RedisUtil._KEY_PREFIX_WEIKADAI_CHANNEL_UID + ":" + date + ":" + channel;
	}

	/**
	 * <pre/>
	 * Type：Hash
	 * Value：指定日期和channel的统计数据
	 * 		field	value
	 * 		pv		{num}
	 * 		uv		{num}
	 * 		newuv	{num}
	 * @param date
	 * @param channel
	 * @return
	 */
	public static String channelStatKey(String date, String channel) {
		return RedisUtil._KEY_PREFIX_WEIKADAI_CHANNEL_STAT + ":" + date + ":" + channel;
	}

	/**
	 * <pre/>
	 * Type：Hash
	 * Value：指定日期的uid归因到的channel
	 * 		field	value
	 * 		{uid}	{channel}
	 * 		{uid}	{channel}
	 * 
	 * @param date
	 * @return
	 */
	public static String uid2ChannelKey(String date) {
		return RedisUtil._KEY_PREFIX_WEIKADAI_UID_CHANNEL + ":" + date;
	}

	/**
	 * <pre/>
	 * Type：Set
	 * Vlaue：当天以前所有存在的uid
	 * 		value
	 * 		{uid}
	 * 		{uid}  
	 */
	public static String pastUidKey() {
		return RedisUtil._KEY_PREFIX_WEIKADAI_UID_PAST;
	}
}

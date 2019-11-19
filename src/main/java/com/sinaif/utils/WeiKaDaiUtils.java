package com.sinaif.utils;

import java.util.Properties;

public class WeiKaDaiUtils {
    public static RedisClient redis;
    public static Properties props;

    static {
        props = ServerUtil.getConfig("wkd.properties");
        redis = RedisClient.getInstance();
    }
}

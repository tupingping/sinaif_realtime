package com.sinaif.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

public class SensorUtils {
    public static MongoClient mongoClient;
    public static MongoDatabase mongoDB;
    public static MongoCollection mongoCol;
    public static Properties props;
    public static Map<String, String> projectMaps;
    public static Map<String, String> eventMaps;
    public static String calStartDate;
    public static int calStartTime;

    static {
        props = ServerUtil.getConfig("sensor.properties");
        calStartDate = props.getProperty("kafka.calc.startDate");
        calStartTime = DateUtil.unixTimestamp(calStartDate,  "yyyyMMdd");
        projectMaps = new HashMap<>();
        eventMaps = new HashMap<>();
        initMongo();
        initMaps("sensor.product.", projectMaps);
        initMaps("sensor.event.", eventMaps);

    }

    private static void initMongo() {
        String host = props.getProperty("mongo.host");
        int port = Integer.parseInt(props.getProperty("mongo.port"));
        String user = props.getProperty("mongo.user");
        String pwd = props.getProperty("mongo.pwd");
        String dbname = props.getProperty("mongo.dbname");
        String collection = props.getProperty("mongo.colletion");
        ServerAddress serverAddress = new ServerAddress(host, port);
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        addrs.add(serverAddress);

        if (StringUtil.isEmpty(user.trim())) {
            mongoClient = new MongoClient(addrs);
        } else {
            // MongoCredential.createScramSha1Credential();
            // 三个参数分别为 用户名 数据库名称 密码
            MongoCredential credential = MongoCredential.createScramSha1Credential(user, dbname, pwd.toCharArray());
            List<MongoCredential> credentials = new ArrayList<>();
            credentials.add(credential);
            //通过连接认证获取MongoDB连接
            mongoClient = new MongoClient(addrs, credentials);
        }
        System.out.println(mongoClient.getMongoClientOptions().getWriteConcern());
        mongoDB = mongoClient.getDatabase(dbname);
        mongoCol = mongoDB.getCollection(collection);
    }

    private static void initMaps(String prefix, Map<String, String> maps) {
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                maps.put(key.substring(prefix.length()), props.getProperty(key));
            }
        }
    }

    public static void upsertWithRetry(String rawData, Bson filter, Bson update) {
        UpdateOptions options = new UpdateOptions().upsert(true);
        try {
            mongoCol.updateOne(filter, update, options);
        } catch (MongoWriteException e) {
            if (e.getMessage().contains("E11000 duplicate key")) {
                // sleep and try again
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                System.out.println(">>>>>>>>>> rawData: " + rawData);
                mongoCol.updateOne(filter, update, options);
            }
        }
    }

}

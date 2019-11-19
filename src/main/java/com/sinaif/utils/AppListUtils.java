package com.sinaif.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Time : 2019/10/9 14:23
 * @Author : pingping.tu
 * @File : AppListUtils.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class AppListUtils {
    public static Properties props;
    public static MongoClient mongoClient;
    public static MongoDatabase mongoDatabase;
    public static MongoCollection mongoCollection;

    static {
        props = ServerUtil.getConfig("applist.properties");
        initMongo();
    }

    private static void initMongo(){
        String host = props.getProperty("mongo.host");
        int port = Integer.parseInt(props.getProperty("mongo.port"));
        String user = props.getProperty("mongo.user");
        String pwd = props.getProperty("mongo.pwd");
        String dbname = props.getProperty("mongo.dbname");
        String collection = props.getProperty("mongo.colletion");

        ServerAddress serverAddress = new ServerAddress(host, port);
        List<ServerAddress> adds = new ArrayList<>();
        adds.add(serverAddress);

        if(StringUtil.isEmpty(user.trim())){
            mongoClient = new MongoClient(adds);
        }else{
            MongoCredential credential = MongoCredential.createScramSha1Credential(user, dbname, pwd.toCharArray());
            List<MongoCredential> credentials = new ArrayList<>();
            credentials.add(credential);

            mongoClient = new MongoClient(adds, credentials);
        }

        System.out.println(mongoClient.getMongoClientOptions().getWriteConcern());
        mongoDatabase = mongoClient.getDatabase(dbname);
        mongoCollection = mongoDatabase.getCollection(collection);
    }

    public static void upsert(String raw, Bson filter, Bson update){
        try {
            UpdateOptions options = new UpdateOptions().upsert(true);
            mongoCollection.updateOne(filter, update, options);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(raw);
        }

    }
}

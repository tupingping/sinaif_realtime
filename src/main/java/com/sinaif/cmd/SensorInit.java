package com.sinaif.cmd;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;
import static com.sinaif.utils.SensorUtils.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.sinaif.utils.DateUtil;
import com.sinaif.utils.StringUtil;

/**
 * load data  from hive, and calculate ,then insert into mongo
 */
public class SensorInit {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String hiveUrl;
    private static String hiveUser;
    private static String hivePwd;
    private static BulkWriteOptions bulkWriteOptions;
    static {
        hiveUrl = props.getProperty("hive.jdbc.url");
        hiveUser = props.getProperty("hive.jdbc.user");
        hivePwd = props.getProperty("hive.jdbc.pwd");
        bulkWriteOptions = new BulkWriteOptions().ordered(false);
    }


    /**
     * # table.2001 = sinaif_king_log.sensors_king_events
     * # table.1003 = sinaif_king_log.sensors_wkd_events
     * create table if not exists sinaif_king_log.sensors_event_stat
     * (distinct_id string,
     * event string,
     * times int
     * )partitioned by (productid string)
     * insert overwrite table sinaif_king_log.sensors_event_stat partition(productid='2001')
     * select distinct_id, event, count(1) as times from sinaif_king_log.sensors_king_events where dt < '20191001'
     * group by distinct_id, event;
     * <p>
     * insert overwrite table sinaif_king_log.sensors_event_stat partition(productid='1003')
     * select distinct_id, event, count(1) as times from sinaif_king_log.sensors_wkd_events where dt < '20191001'
     * group by distinct_id, event;
     */
    public static void hive2Mongo(String productId, int batchSize) {
        try {
            Class.forName(driverName);
            Connection con = DriverManager.getConnection(hiveUrl, hiveUser, hivePwd);
            Statement stmt = con.createStatement();
            String statTable = "sinaif_king_log.sensors_event_stat";
//            String statSql = "insert overwrite table " + statTable + " partition (productid = '" + productId + "')" +
//                    " select distinct_id, event, count(1) as times from " + hiveTable + " where dt < '" + endDate + "'" +
//                    " group by distinct_id, event";

//            System.out.println(curDate + ", start hive stat history data, hiveTable=" + hiveTable + ", productid=" + productId + ", endDate=" + endDate);
//            stmt.execute(statSql);
//            curDate = DateUtil.currentDate("yyyy-MM-dd hh:mm:ss");
//            System.out.println(curDate+ ", end hive stat history data, hiveTable=" + hiveTable + ", productid=" + productId + ", endDate=" + endDate);

            String query = "select distinct_id, event, times from " + statTable + " where productid = '" + productId + "'";
            ResultSet res = stmt.executeQuery(query);
            int processNum = 0;
            List<WriteModel> list = new ArrayList<>();

            System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", start load hive data to mongo");
            while (res.next()) {
                String userid = res.getString("distinct_id");
                String event = res.getString("event");
                int times = res.getInt("times");
                String statItem = eventMaps.get(event);
                processNum++;
                long startTs;
                if (null != statItem) {
                    list.add(genUpdateOneModel(productId, userid, statItem, times));
                }
                if (processNum > 0 && processNum % batchSize == 0) {
                    if (list.size() > 0) {
                        startTs = System.currentTimeMillis();
                        System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", bulkWrite start ");
                        BulkWriteResult result = mongoCol.bulkWrite(list, bulkWriteOptions);
                        long cost = System.currentTimeMillis() - startTs;
                        System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", bulkWrite end, processNum: " + processNum + ", "
                                + result.getMatchedCount() + "," + result.getInsertedCount() + "," + result.getModifiedCount() + ", cost: " + cost);
                        list.clear();
                    }
                }
            }
            if (list.size() > 0) {
                mongoCol.bulkWrite(list, bulkWriteOptions);
            }
            System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", end load hive data to mongo, process num: " + processNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void local2Mongo(String productId, int batchSize, String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));

            int processNum = 0;
            List<WriteModel> list = new ArrayList<>();

            System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", start load local file data to mongo");
            String line;
            while (null != (line = reader.readLine())) {
                String[] arr = line.split("\t");
                if (arr.length != 3) {
                    continue;
                }
                String userid = arr[0];
                String event = arr[1];
                int times = Integer.parseInt(arr[2]);
                String statItem = eventMaps.get(event);
                processNum++;
                long startTs;
                if (null != statItem) {
                    list.add(genUpdateOneModel(productId, userid, statItem, times));
                }
                if (processNum > 0 && processNum % batchSize == 0) {
                    if (list.size() > 0) {
                        startTs = System.currentTimeMillis();
                        System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", bulkWrite start ");
                        BulkWriteResult result = mongoCol.bulkWrite(list, bulkWriteOptions);
                        long cost = System.currentTimeMillis() - startTs;
                        System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", bulkWrite end, processNum: " + processNum + ", "
                                + result.getMatchedCount() + "," + result.getInsertedCount() + "," + result.getModifiedCount() + ", cost: " + cost);
                        list.clear();
                    }
                }
            }
            if (list.size() > 0) {
                mongoCol.bulkWrite(list, bulkWriteOptions);
            }
            System.out.println(">>>>>>>>>> " + DateUtil.currentDateTime() + ", end load local file to mongo, process num: " + processNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static UpdateOneModel genUpdateOneModel(String productid, String userid, String statItem, int value) {
        Bson filter = Filters.and(eq("productid", productid), eq("userid", userid));
        //指定修改的更新文档
        String uptime = DateUtil.currentDate("yyyy-MM-dd HH:mm:ss");
        Bson update = combine(setOnInsert("crtime", uptime),
                set("click_event_stat." + statItem, value),
                set("click_event_stat.uptime", uptime),
                set("uptime", uptime));
        UpdateOptions options = new UpdateOptions();
        options.upsert(true);
        return new UpdateOneModel(filter, update, options);
    }

    public static void main(String[] args) {
        // table, productid, endDate
        if (args.length < 2) {
            System.out.println("Usage java -cp sinaif_realtime_sensor.jar com.sinaif.cmd.SensorInit 2001 1000 localFile");
        } else {
            String productId = args[0];
            int batchSize = StringUtil.convertInt(args[1], 1000);
            if (args.length == 3) {
                String fileName = args[2];
                local2Mongo(productId, batchSize, fileName);
            } else {
                hive2Mongo(productId, batchSize);
            }
        }
    }
}

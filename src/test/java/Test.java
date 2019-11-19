import static com.mongodb.client.model.Updates.*;
import static com.sinaif.utils.SensorUtils.*;
import static com.sinaif.utils.StringUtil.jsonParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.bson.conversions.Bson;

import com.google.gson.JsonObject;
import com.mongodb.client.model.Filters;
import com.sinaif.utils.DateUtil;
import com.sinaif.utils.StringUtil;

public class Test {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("d:\\kafka.txt")));
        String line;
        while (null != (line = br.readLine())){
            processMsg(line);
        }
    }
    public static void processMsg(String raw) {
        /**
         * kafka数据流非标准json格式，数据里面乱码/url编码混乱
         */
        // .replace("\\\"", "\"").replace("\\\\", "").replace("\\{", "{").replace("}\"",
        // "}").replace("\\[", "[").replace("]\"", "]");
        // System.out.println(x);
        String distinctId = "";
        String event = "";
        String project = "";
        Long time;
        try {
            JsonObject rawObj = jsonParser.parse(raw).getAsJsonObject();
            event = rawObj.has("event") ? rawObj.get("event").getAsString() : null;
            distinctId = rawObj.has("distinct_id") ? rawObj.get("distinct_id").getAsString() : null;
            project = rawObj.has("project") ? rawObj.get("project").getAsString() : null;
            time = Long.parseLong(rawObj.get("time").getAsString());
            if(event == null || distinctId == null || project == null){
                return;
            }
            String mongoProductid = projectMaps.get(project);
            String mongoStatField = eventMaps.get(event);
            if ((!StringUtil.isEmpty(mongoProductid)) && (!StringUtil.isEmpty(mongoStatField))) {
                if (time/1000 < calStartTime) {
                    // 初始化完成之前的消息，直接跳过
                    return;
                }
                String uptime = DateUtil.currentDate("yyyy-MM-dd HH:mm:ss");
                Bson filter = Filters.and(Filters.eq("productid", mongoProductid), Filters.eq("userid", distinctId));
                Bson update = combine(setOnInsert("crtime", uptime),
                        inc("click_event_stat." + mongoStatField, 1),
                        set("click_event_stat.uptime", uptime),
                        set("uptime", uptime));
                upsertWithRetry(raw, filter, update);
            }
        } catch (Exception e) {
            System.out.println(raw);
            e.printStackTrace();
        }
    }
}

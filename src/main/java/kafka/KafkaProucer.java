package kafka;

import com.sinaif.utils.DateUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * version 0.11.0.0 kafka
 * Proucer Demo
 * @author dell
 *
 */
public class KafkaProucer {

	public static void main(String[] args) throws IOException, Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "master:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		String topic = "weikadai_click_stream";
		Producer<String, String> producer = new KafkaProducer<>(props);
		BufferedReader reader = new BufferedReader(new FileReader(new File("d:\\wkd_msg.log")));
		String line = "";
		int key = 1;
		String curDate = DateUtil.currentDate("yyyy-MM-dd");
		while ((line = reader.readLine()) != null) {
			line = line.replace("2019-05-29", curDate);/**/
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(key), line));
			if(key%100==0) {
				Thread.sleep(100);
			}
			key++;
		}
		System.out.println(key);
		reader.close();
		producer.close();
	}
}

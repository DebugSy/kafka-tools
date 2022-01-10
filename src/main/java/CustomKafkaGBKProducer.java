import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Created by P0007 on 2020/03/09.
 */
@Slf4j
public class CustomKafkaGBKProducer implements Runnable {

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.1.17:9094";

    private static String topic = "shiy";

    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public void run() {
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        produce(kafkaProducer, topic);
    }


    public static void main(String[] args) {

        CustomKafkaGBKProducer kafkaProducerTool = new CustomKafkaGBKProducer();
        kafkaProducerTool.run();
    }

    public KafkaProducer initProducer(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("batch.size", "16384");
        props.put("linger.ms", "1");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    public void produce(KafkaProducer producer, String topic) {
        String message = "65,userB,http://127.0.0.1/api/I,2021-11-25 09:26:25.303,38,88cd75c4-3bd7-408c-93c4-6d64d795fd2a,20211125,092625";
//        String message = "{\"userId\": 66,\"username\": \"用户B\",\"url\": \"http://127.0.0.1/api/I\",\t\"clickTime\": \"2021-11-25 09:26:25.303\",\t\"user_rank\": 38,\t\"uuid\": \"88cd75c4-3bd7-408c-93c4-6d64d795fd2a\",\t\"dateStr\": \"20211125\",\t\"timeStr\": \"092625\"}";
        ProducerRecord<String, byte[]> record = null;
        try {
            record = new ProducerRecord<String, byte[]>(topic, message.getBytes("GBK"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        producer.send(record);
        try {
            Thread.sleep(3 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

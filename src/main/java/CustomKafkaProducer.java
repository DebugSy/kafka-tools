import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Created by P0007 on 2020/03/09.
 */
@Slf4j
public class CustomKafkaProducer implements Runnable {

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.1.82:9094";

    private static String topic = "shiy.flink.multiple.message";

    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public void run() {
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        produce(kafkaProducer, topic);
    }


    public static void main(String[] args) {

        CustomKafkaProducer kafkaProducerTool = new CustomKafkaProducer();
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
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    public void produce(KafkaProducer producer, String topic) {
        String message = new String("CS_AIU_MTC`66,userB,http://127.0.0.1/api/I,2021-11-25 09:26:25.303,38,88cd75c4-3bd7-408c-93c4-6d64d795fd2a,20211125,092625\n" +
                "67,userC,http://127.0.0.1/api/K,2021-11-25 09:26:26.379,31,87b4e551-44ce-456b-9d84-a1f6fbe69ed4,20211125,092626\n" +
                "73,userI,http://127.0.0.1/api/K,2021-11-25 09:27:04.269,42,44eae444-26ae-4073-ae8b-3a72e8e2c5dc,20211125,092704\n" +
                "69,userE,http://127.0.0.1/api/H,2021-11-25 09:27:05.380,44,60b600f9-eca7-4db9-af9c-e2ffb3dde4a5,20211125,092705");
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
        producer.send(record);
        try {
            Thread.sleep(3 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

package com.shiy.tools.kafka.snowball;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by P0007 on 2020/03/09.
 */
@Slf4j
public class SnowballKafkaProducer implements Runnable{

    private static final String SEPARATOR = ",";

    private static String bootstrap = "rf.test.bigdata.node2:9092";

    private static String topic = "shiy.flink.snowball.sink.2";

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public void run() {
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        produce(kafkaProducer, topic);
    }


    public static void main(String[] args) {

        SnowballKafkaProducer kafkaProducerTool = new SnowballKafkaProducer();
        for (int i = 0; i < 1; i++) {
            Thread thread = new Thread(kafkaProducerTool);
            thread.setName("Thread-" + i);
            thread.start();
        }
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

//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.kerberos.service.name", "kafka");
//        props.put("sasl.mechanism", "GSSAPI");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    public void produce(KafkaProducer producer, String topic) {
        Random random = new Random(System.currentTimeMillis());
        int count = 0;
        while (true) {
            try {
                int sourceFlag = random.nextInt(2);
                StringBuilder message;
                switch (sourceFlag) {
                    case 0:
                    case 1:
                        message = generateUrlClickMessage();
                        break;
                    default:
                        throw new RuntimeException("sourceFlag is error");
                }
                log.info("{}", message.toString());
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                        topic, 4, UUID.randomUUID().toString().substring(0, 6), message.toString());
                producer.send(record);
                count++;
                if (count == 2000000) {
                    count = 0;
                    Thread.sleep(1000 * 5);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private StringBuilder generateUrlClickMessage() {
        Random random = new Random(System.currentTimeMillis());
        int nextInt = random.nextInt(100);
        String action = "I";
        Integer userId = 65 + nextInt;
        String username = "user" + (char) ('A' + nextInt);
        Timestamp clickTime = new Timestamp(System.currentTimeMillis());
        LocalDateTime localDateTime = clickTime.toLocalDateTime();
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
        String clickTimeStr = dateTimeFormatter.format(zonedDateTime);
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
        return new StringBuilder()
                .append(action)
                .append(SEPARATOR).append(userId)
                .append(SEPARATOR).append(username)
                .append(SEPARATOR).append(url)
                .append(SEPARATOR).append(clickTimeStr)
                .append(SEPARATOR).append(random.nextInt(100))
                .append(SEPARATOR).append(UUID.randomUUID().toString())
                .append(SEPARATOR).append(dateStr)
                .append(SEPARATOR).append(timeStr);
    }

}

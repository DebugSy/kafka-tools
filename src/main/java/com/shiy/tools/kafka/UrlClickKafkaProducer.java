package com.shiy.tools.kafka;

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
public class UrlClickKafkaProducer implements Runnable{

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.1.82:9094";

    private static String topic = "shiy.flink.url.click";

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public void run() {
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        produce(kafkaProducer, topic);
    }


    public static void main(String[] args) {

//        System.setProperty("java.security.krb5.conf", "E:\\kerberos\\wangjing\\krb5.conf");
//        System.setProperty("java.security.auth.login.config", "E:\\kerberos\\wangjing\\kafka_client_jaas.conf");

        UrlClickKafkaProducer kafkaProducerTool = new UrlClickKafkaProducer();
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
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message.toString());
                producer.send(record);
                count++;
                Thread.sleep(1000 * 1);
                if (count == 1000) {
                    Thread.sleep(1000 * 5);
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private StringBuilder generateUrlClickMessage() {
        Random random = new Random(System.currentTimeMillis());
        int nextInt = random.nextInt(10);
        Integer userId = 65 + nextInt;
        String username = "user" + (char) ('A' + nextInt);
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000);
        LocalDateTime localDateTime = clickTime.toLocalDateTime();
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
        String clickTimeStr = dateTimeFormatter.format(zonedDateTime);
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        String url = "http://127.0.0.1/api/" + (char) ('H' + random.nextInt(4));
        return new StringBuilder()
                .append(userId)
                .append(SEPARATOR).append(username)
                .append(SEPARATOR).append(url)
                .append(SEPARATOR).append(clickTimeStr)
                .append(SEPARATOR).append(random.nextInt(100))
                .append(SEPARATOR).append(UUID.randomUUID().toString())
                .append(SEPARATOR).append(dateStr)
                .append(SEPARATOR).append(timeStr);
    }

}

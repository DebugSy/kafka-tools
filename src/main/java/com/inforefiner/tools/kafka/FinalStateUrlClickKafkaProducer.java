package com.inforefiner.tools.kafka;

import com.inforefiner.tools.kafka.generator.FiniStateUrlGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by P0007 on 2020/03/09.
 */
public class FinalStateUrlClickKafkaProducer implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(FinalStateUrlClickKafkaProducer.class);

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.2.170:9092";

    private static String topic = "shiy_topic_final_state";

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");

    public void run() {
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        produce(kafkaProducer, topic);
    }


    public static void main(String[] args) {
        FinalStateUrlClickKafkaProducer kafkaProducerTool = new FinalStateUrlClickKafkaProducer();
        FiniStateUrlGenerator.init();
        for (int i = 0; i < 50; i++) {
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
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    public void produce(KafkaProducer producer, String topic) {
        int count = 0;
        while (true) {
            try {
                String message = FiniStateUrlGenerator.generateUrlClickDate();
                log.info(message);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
                producer.send(record);
                count++;
                if (count == 1) {
                    Thread.sleep(1000 * 10 * 1);
                    count = 0;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

package com.inforefiner.tools.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * Created by P0007 on 2019/9/26.
 */
public class KafkaProducerTool {

    private static final String SEPARATOR = ",";

    private static String bootstrap = "localhost:9092";

    private static String topic = "ZYJK_PACKAGE_DATA";

    //用户
    private List<String> userIdPool = new ArrayList<String>();

    //供应商
    private List<String> supplierPool = new ArrayList<String>();

    public static void main(String[] args) {
        KafkaProducerTool kafkaProducerTool = new KafkaProducerTool(100, 50);
        KafkaProducer kafkaProducer = kafkaProducerTool.initProducer(bootstrap);
        kafkaProducerTool.produce(kafkaProducer, topic);
    }

    public KafkaProducerTool(int userIdPoolSize, int supplierPoolSize) {
        for (int i = 1; i <= userIdPoolSize; i++) {
            userIdPool.add("u" +  String.format("%03d", i));
        }
        for (int i = 1; i <= supplierPoolSize; i++) {
            supplierPool.add("p" + String.format("%03d", i));
        }
    }

    private void paserArgs(String[] args) {

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
        long id = 0;
        while (true) {
            try {
                ++id;
                String message = prepareMessage(id);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
                System.out.println("Produce message -> " + record.toString());
                producer.send(record);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String prepareMessage(long id) {
        StringBuffer stringBuffer = new StringBuffer();
        Random random = new Random(System.currentTimeMillis());

        stringBuffer.append(id).append(SEPARATOR)
                .append(System.currentTimeMillis()).append(SEPARATOR)
                .append(userIdPool.get(random.nextInt(userIdPool.size()))).append(SEPARATOR)
                .append(supplierPool.get(random.nextInt(supplierPool.size()))).append(SEPARATOR)
                .append(random.nextInt(1000));
        return stringBuffer.toString();
    }
}

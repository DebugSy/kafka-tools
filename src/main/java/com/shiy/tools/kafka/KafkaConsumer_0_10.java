package com.shiy.tools.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by DebugSy on 2017/7/31.
 */
public class KafkaConsumer_0_10 {

    public static void main(String[] args) {

//        System.setProperty("java.security.krb5.conf", "E:\\kerberos\\sandbox\\krb5.conf");
//        System.setProperty("java.security.auth.login.config", "E:\\kerberos\\sandbox\\kafka_server_jaas.conf");
//        System.setProperty("java.security.krb5.conf", "E:\\kerberos\\wangjing\\krb5.conf");
//        System.setProperty("java.security.auth.login.config", "E:\\kerberos\\wangjing\\kafka_client_jaas.conf");

//        System.setProperty("sun.security.krb5.debug", "true");

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.82:9094");
//        props.put("bootstrap.servers", "192.168.21.203:6667,192.168.21.204:6667,192.168.21.205:6667");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.kerberos.service.name", "kafka");
//        props.put("sasl.mechanism", "GSSAPI");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("shiy.flink.multiple.message"));//订阅topic


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, partition=%d, timestamp=%s, timestampType=%s, key = %s, value = %s",
                        record.offset(), record.partition(), dateFormat.format(new Date(record.timestamp())), record.timestampType(), record.key(), record.value());
                System.out.println();
            }
        }

    }


}

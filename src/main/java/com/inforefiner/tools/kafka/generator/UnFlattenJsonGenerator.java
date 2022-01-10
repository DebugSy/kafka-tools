package com.inforefiner.tools.kafka.generator;

import com.inforefiner.tools.kafka.JsonBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class UnFlattenJsonGenerator implements Runnable{

    private static JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    private static String bootstrap = "info3:6667,info4:6667,info5:6667";

    private static String topic = "shiy.flink.es.json";

    private static int thread = 10; //多少线程

    private final KafkaProducer kafkaProducer;

    public UnFlattenJsonGenerator() {
        this.kafkaProducer = initProducer(bootstrap);
    }

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(thread);
        for (int i = 0; i < thread; i++) {
            UnFlattenJsonGenerator generator = new UnFlattenJsonGenerator();
            Thread thread = new Thread(generator);
            thread.setName("Thread-" + i);
            thread.start();
            countDownLatch.countDown();
        }
        countDownLatch.await();
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

    @Override
    public void run() {
        Person person = new Person();
        Random random = new Random(System.currentTimeMillis());
        long count = 0;
        while (true){
            int nextInt = random.nextInt(5);
            person.setId(65 + nextInt);
            person.setName("user" + (char) ('a' + nextInt));
            person.setPhones(new Long[]{15545315615L, 15466823545L});
            Address address = new Address();
            AddressDesc firstAddress = new AddressDesc();
            firstAddress.setAddress1("address_1_" + nextInt);
            firstAddress.setAddress2("address_2_" + nextInt);
            AddressDesc secondAddress = new AddressDesc();
            String json = "{\"logindexname_lower\":\"gd_h3c_st_run_sw_syslog\",\"NE_name\":\"NF_SHT_M_CE03_H3C_CR16010\",\"IpAddr\":\"10.242.131.142\",\"User\":\"suyan\",\"NE_ip\":\"10.242.131.225\",\"vender\":\"H3C\",\"DevIp\":\"10.242.131.225\",\"log_type\":\"RUN\",\"collect_type\":\"syslog\",\"NE_type\":\"SW\",\"es_index\":\"gd_h3c_st_run_sw_syslog-20210628\",\"timestamp\":\"2021-06-28T14:08:27.000+08:00\",\"severity\":\"6\",\"log_name\":\"syslog\",\"collect_ip\":\"192.168.186.18\",\"modul\":\"%%10SSHS\",\"message\":\"<190>Jun 28 14:08:27 2021NF-SHT-M-CE03-H3C-CR16010%%10SSHS/6/SSHS_DISCONNECT:-DevIP=10.242.131.225;SSHusersuyan(IP:10.242.131.142)disconnectedfromtheserver.\\u0000\",\"logIndexName\":\"gd_h3c_st_run_sw_syslog\",\"KafkaTopic\":\"GD_ET_OTHER\",\"subType\":\"SSHS_DISCONNECT\",\"collect_time\":\"2021-06-28T06:08:28.067Z\"}";
            secondAddress.setAddress1(json);
            address.setFirstAddress(firstAddress);
            address.setSecondAddress(secondAddress);
            person.setAddress(address);
            String toJson = jsonBuilder.toJson(person);
            log.info("emit {} records, {}", ++count, toJson);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, 2, System.currentTimeMillis(),
                    UUID.randomUUID().toString().substring(0, 6),toJson);
            kafkaProducer.send(record);
            if (count == 100000) {
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                kafkaProducer.close();
                break;
            }
        }
    }
}

@Getter
@Setter
class Person {
    private int id;
    private String name;
    private Long[] phones;
    private Address address;

}

@Getter
@Setter
class Address {
    private AddressDesc firstAddress;
    private AddressDesc secondAddress;
}

@Getter
@Setter
class AddressDesc {
    private String address1;
    private String address2;
}

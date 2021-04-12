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

@Slf4j
public class UnFlattenJsonGenerator {

    private static JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    private static String bootstrap = "192.168.1.82:9094";

    private static String topic = "shiy.flink.json";

    public static void main(String[] args) throws InterruptedException {

        UnFlattenJsonGenerator generator = new UnFlattenJsonGenerator();
        KafkaProducer kafkaProducer = generator.initProducer(bootstrap);

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
            secondAddress.setAddress1("address_1_" + nextInt + "_" + UUID.randomUUID().toString().substring(0,6));
            address.setFirstAddress(firstAddress);
            address.setSecondAddress(secondAddress);
            person.setAddress(address);
            String toJson = jsonBuilder.toJson(person);
            log.info("emit {} records, {}", ++count, toJson);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, toJson);
            kafkaProducer.send(record);
            Thread.sleep(1000 * 5);
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

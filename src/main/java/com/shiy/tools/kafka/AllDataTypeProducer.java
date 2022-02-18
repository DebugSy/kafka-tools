package com.shiy.tools.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class AllDataTypeProducer {

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.1.82:9094";

    private static String topic = "shiy.flink.all.datatype";

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random(System.currentTimeMillis());
        KafkaProducer kafkaProducer = initProducer(bootstrap);
        while (true) {
            produce(kafkaProducer, topic);
        }
    }

    public static KafkaProducer initProducer(String bootstrap) {
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

    public static void produce(KafkaProducer producer, String topic) {
        Random random = new Random(System.currentTimeMillis());
        int count = 0;
        while (true) {
            try {
                String message = generateAllDataTypeMessage(random);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
                log.info("Produce message -> {}", record.toString());
                producer.send(record);
                count++;
                if (count == 1) {
                    Thread.sleep(1000 * 1 * 1);
                    count = 0;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static String generateAllDataTypeMessage(Random random) {
        int intColV = random.nextInt(5);
        String stringColV = intColV % 2 == 0 ? "I" : "U";
        boolean booleanColV = random.nextBoolean();
        byte byteColV = Byte.MAX_VALUE;
        Timestamp timestampColV = new Timestamp(System.currentTimeMillis() - 7171000);
        Date dateColV = new Date(timestampColV.getTime());
        BigDecimal decimalColV = generateDecimal(random.nextDouble(), intColV);
        double doubleColV = random.nextDouble();
        float floatColV = random.nextFloat();
        long longColV = random.nextLong();
        short shortCloV = Short.MAX_VALUE;
        byte[] binaryColV = new byte[8];
        Arrays.fill(binaryColV, Byte.MIN_VALUE);

        return new StringBuilder()
                .append(intColV)
                .append(SEPARATOR).append(stringColV)
                .append(SEPARATOR).append(booleanColV)
                .append(SEPARATOR).append(byteColV)
                .append(SEPARATOR).append(timestampColV)
                .append(SEPARATOR).append(dateColV)
                .append(SEPARATOR).append(decimalColV)
                .append(SEPARATOR).append(doubleColV)
                .append(SEPARATOR).append(floatColV)
                .append(SEPARATOR).append(longColV)
                .append(SEPARATOR).append(shortCloV)
                .toString();
    }

    /**
     * 生成bigDecimal类型
     * @param number 数值
     * @param scale 小数位数
     * @return
     */
    private static BigDecimal generateDecimal(double number, int scale) {
        NumberFormat numberFormat = NumberFormat.getNumberInstance();
        numberFormat.setMinimumFractionDigits(scale);
        String decimalCol1Format = numberFormat.format(number);
        BigDecimal bigDecimal = new BigDecimal(decimalCol1Format);
        return bigDecimal;
    }

}

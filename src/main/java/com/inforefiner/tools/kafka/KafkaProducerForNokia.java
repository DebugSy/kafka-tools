package com.inforefiner.tools.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by P0007 on 2020/3/3.
 *
 * nokia金科活体数据源模拟
 */
public class KafkaProducerForNokia {

    private static final String SEPARATOR = ",";

    private static String bootstrap = "192.168.2.170:9092";

    private static String topic = "CMMTKSAUTH";

    public static void main(String[] args) {
        KafkaProducerForNokia kafkaProducerTool = new KafkaProducerForNokia();
        KafkaProducer kafkaProducer = kafkaProducerTool.initProducer(bootstrap);
        kafkaProducerTool.produce(kafkaProducer, topic);
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
        while (true) {
            try {
                String message = prepareMessage();
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
                System.out.println("Produce message -> " + record.toString());
                producer.send(record);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String prepareMessage() {
        StringBuffer stringBuffer = new StringBuffer();
        UUID uuid = UUID.randomUUID();
        Random random = new Random(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
        SimpleDateFormat allFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);

        stringBuffer
                .append("FACEID-HMJ-" + uuid.toString().replaceAll("-", "")).append(SEPARATOR) //唯一流水号
                .append(random.nextDouble()).append(SEPARATOR) //分数
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为千分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为万分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为十万分之一的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 误识率为百万分之一的置信度阈值
                .append("https://mca.cmpay.com:28710/fsgroup1/M00/C7/44"+random.nextLong()+".jpg").append(SEPARATOR) // 图片路径
                .append("Result description " + random.nextLong()).append(SEPARATOR) //结果描述
                .append(random.nextInt(3)).append(SEPARATOR) // 审核结果
                .append(random.nextLong()).append(SEPARATOR) //用户号
                .append(random.nextLong()).append(SEPARATOR) //手机号
                .append("username" + random.nextInt()).append(SEPARATOR) // 姓名
                .append(random.nextLong()).append(SEPARATOR) // 身份证号
                .append(dateFormat.format(date)).append(SEPARATOR) //审核日期
                .append(timeFormat.format(date)).append(SEPARATOR) //审核时间
                .append("response json").append(SEPARATOR) // 返回报文
                .append("1").append(SEPARATOR) // 认证类型
                .append(System.currentTimeMillis()).append(SEPARATOR) // 时间戳
                .append(allFormat.format(date)).append(SEPARATOR) //请求ID
                .append("error message").append(SEPARATOR) //错误信息
                .append(random.nextLong()).append(SEPARATOR) // 旷视处理时间
                .append(random.nextLong()).append(SEPARATOR) //调用时间
                .append(random.nextInt(2)).append(SEPARATOR) //是否被攻击
                .append(random.nextInt(2)).append(SEPARATOR) //是否彩色照片
                .append("AUTH_BUS_TYP").append(SEPARATOR) // 业务类型
                .append(random.nextInt(10)).append(SEPARATOR) // 系统标识
                .append("https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".jpg").append(SEPARATOR) // 最佳人脸图片地址
                .append("https://mca.cmpay.com:28710/fsgroup1/"+random.nextLong()+".txt").append(SEPARATOR) // delta数据文件地址
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为软件合成脸的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为软件合成脸的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为面具的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为面具的置信度阈值
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为屏幕翻拍的置信度
                .append(random.nextDouble()).append(SEPARATOR) // 表示人脸照片为屏幕翻拍的置信度阈值
                .append(random.nextInt(2)).append(SEPARATOR); // 是否换脸攻击
        return stringBuffer.toString();
    }

}

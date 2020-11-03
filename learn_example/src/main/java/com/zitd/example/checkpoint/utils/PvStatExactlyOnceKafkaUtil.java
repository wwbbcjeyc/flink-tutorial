package com.zitd.example.checkpoint.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
  *用于给 PvStatExactlyOnce 生成数据, 并统计数据集中不同数据的个数
 */
public class PvStatExactlyOnceKafkaUtil {
    public static final String broker_list = "192.168.30.215:9092,192.168.30.216:9092,192.168.30.220:9092";
    private static final HashMap<String, Long> producerMap = new HashMap<>();

    public static final String topic = "app-topic";

    private static void writeToKafka(){
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        // 生成 0~9 的随机数做为 appId
        String value = "" + new Random().nextInt(2);
        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, value);
        producer.send(record);
        System.out.println("发送数据: " + value);

        Long pv = producerMap.get(value);
        if (null == pv) {
            producerMap.put(value, 1L);
        } else {
            producerMap.put(value, pv + 1);
        }
        System.out.println("生产数据:");
        for (Map.Entry<String, Long> appIdPv : producerMap.entrySet()) {
            System.out.println("appId:" + appIdPv.getKey() + "   pv:" + appIdPv.getValue());
        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            writeToKafka();
        }
    }

}

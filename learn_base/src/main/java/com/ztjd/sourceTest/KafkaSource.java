package com.ztjd.sourceTest;

import akka.protobuf.RpcUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        /*properties.setProperty("bootstrap.servers", "172.16.200.37:9092");
        properties.getProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");*/

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"ckafka.xiaobangtouzi.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
       // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("mysql-220", new SimpleStringSchema(), properties);
        /*Map<KafkaTopicPartition, Long> offsets = new HashedMap();
        offsets.put(new KafkaTopicPartition("dwb-b_metis_user_i", 0), 543597315L);
        offsets.put(new KafkaTopicPartition("dwb-b_metis_user_i", 0), 543597316L);
        offsets.put(new KafkaTopicPartition("dwb-b_metis_user_i", 0), 543597317L);
        myConsumer.setStartFromSpecificOffsets(offsets);*///指定offset 消费




        //myConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始
        myConsumer.setStartFromLatest();       // 从最新的记录开始
        //myConsumer.setStartFromTimestamp(1596760381000L); // 从指定的时间开始（毫秒）
        //myConsumer.setStartFromGroupOffsets(); // 默认的方法
        DataStreamSource<String> inputStream = env.addSource(myConsumer);
        inputStream.print();


        env.execute();

    }
}

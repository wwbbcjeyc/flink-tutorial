package com.ztjd.sinkTest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class KafkaSink {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ckafka.xiaobangtouzi.com:9092");
        properties.getProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<ObjectNode> consumer = new FlinkKafkaConsumer010<>("mysql-220", new JSONKeyValueDeserializationSchema(true), properties);

        DataStreamSource<ObjectNode> streamSource = env.addSource(consumer);
        streamSource.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode value) throws Exception {
                JsonNode database = value.get("value").get("database");
                JsonNode table = value.get("value").get("table");

                return database.toString().contains("metis") && table.toString().contains("user_order_info");
            }
        }).print();

        /*SingleOutputStreamOperator<String> process = streamSource.process(new ProcessFunction<ObjectNode, String>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<String> out) throws Exception {
                JsonNode key = value.get("key");
                System.out.println("key"+key);
                JsonNode val = value.get("value");
                System.out.println("value"+val);
                JsonNode database = value.get("value").get("database");
                System.out.println("database"+database);
                JsonNode table = value.get("value").get("table");
                System.out.println("table"+table);
            }
        });*/

       /* final FlinkKafkaConsumer<String> consumer011 = new FlinkKafkaConsumer<>("dwb-b_track_detail_info_i", new SimpleStringSchema(), properties);
        //consumer011.setStartFromEarliest();
        consumer011.setStartFromLatest();
        env.addSource(consumer011).print();*/



        env.execute();
    }
}

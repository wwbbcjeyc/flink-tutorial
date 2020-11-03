package com.ztjd.sourceTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class sourceTest2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.20.20.23:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("sensor", new SimpleStringSchema(), properties);

         DataStreamSource<String> sourceDataStream = env.addSource(myConsumer);
         
         sourceDataStream.map(x->JSON.parseObject(x))
                 .flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                     @Override
                     public void flatMap(JSONObject event, Collector<JSONObject> out) throws Exception {
                         JSONObject flatResult = new JSONObject();
                         String flow_attribute_info = event.getString("flow_attribute_info");
                         JSONArray jsonArray = JSON.parseArray(flow_attribute_info);

                         for (int i = 0; i < jsonArray.size(); i++) {
                             JSONObject jsonObject = jsonArray.getJSONObject(i);
                             flatResult.put("flow_attribute_type",event.get("flow_attribute_type"));
                             flatResult.put("parent_source_code",event.get("parent_source_code"));
                             flatResult.put("flow_attribute_type_code",event.get("flow_attribute_type_code"));
                             flatResult.put("flow_name",event.get("flow_name"));
                             flatResult.put("created_at",event.get("created_at"));
                             flatResult.put("flow_business_scene_code",event.get("flow_business_scene_code"));
                             flatResult.put("flow_source_id",event.get("flow_source_id"));
                             flatResult.put("flow_attribute_id",jsonObject.get("flow_attribute_id"));
                             flatResult.put("flow_id",event.get("flow_id"));
                             flatResult.put("flow_attribute_value",jsonObject.get("flow_attribute_value"));
                             flatResult.put("flow_operation_id",event.get("flow_operation_id"));
                             flatResult.put("flow_source_code",event.get("flow_source_code"));
                             flatResult.put("flow_category_id",event.get("flow_category_id"));
                             flatResult.put("flow_category_code",event.get("flow_category_code"));
                             flatResult.put("status",event.get("status"));
                             out.collect(flatResult);
                         }

                     }
                 }).print("flatMap:");
         env.execute();
    }
}

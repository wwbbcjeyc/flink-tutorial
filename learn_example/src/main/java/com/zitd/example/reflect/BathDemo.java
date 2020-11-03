package com.zitd.example.reflect;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class BathDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.200.30:6667");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<JSONObject> student = env.addSource(new FlinkKafkaConsumer010<String>("student", new SimpleStringSchema(), props))
                .map(value -> JSONObject.parseObject(value));
       // student.print();

        SingleOutputStreamOperator<List<JSONObject>> windowDataStream = student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<JSONObject> values, Collector<List<JSONObject>> out) throws Exception {
                ArrayList<JSONObject> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    log.info("1 分钟内收集到 student 的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        }).setParallelism(1);
       // windowDataStream.print();
        windowDataStream.addSink(new JDBCFunction());

        env.execute("connectors mysql");
    }
}

package com.ztjd.sourceTest;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;


public class SourceTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



       env.fromElements(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_2", 1547718201L, 15.4),
                new SensorReading("sensor_3", 1547718202L, 6.7),
                new SensorReading("sensor_4", 1547718205L, 38.1)
        );

        env.readTextFile("E:\\Project\\java\\fink-tutorial\\src\\main\\resources\\hello.txt");

        //从kafka
        Properties pro = new Properties();

        pro.setProperty("bootstrap.servers", "192.168.1.4:9092");
        pro.getProperty("group.id", "consumer-group");
        pro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>("sensor", new SimpleStringSchema(), pro);
        DataStreamSource<String> dataStream = env.addSource(kafkaConsumer010);
        dataStream.print();



        env.execute();


    }
}

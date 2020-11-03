package com.ztjd.sourceTest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class SourceTest1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数组作为数据输入源
        String[] elementInput = new String[]{"hello flink", "hello spark"};
        DataStreamSource<String> text = env.fromElements(elementInput);

        // 添加List集合作为数据输入源

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_2", 1547718201L, 15.4),
                        new SensorReading("sensor_3", 1547718202L, 6.7),
                        new SensorReading("sensor_4", 1547718205L, 38.1)
                )
        );


        // 指定 CsvInputFormat, 监控csv文件(两种模式), 时间间隔是10ms
        /*DataStreamSource<String> testcsv = env.readFile(new CsvInputFormat<String>(new Path("")) {

            @Override
            protected String fillRecord(String s, Object[] objects) {
                return null;
            }
        }, "", FileProcessingMode.PROCESS_CONTINUOUSLY, 10);
*/

        text.print();
        env.execute("Inside DataSource Demo");
    }
}

package com.ztjd.processFun;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class SideOutputTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) input -> {
            String[] dataArray = input.split(",");
            return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
        });

        // 利用process function得到侧输出流，做分流操作

        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new SplitTemp(30.0));

        // 获取侧输出流数据

        DataStream<Tuple3<String, Long, Double>> lowTempStream = highTempStream.getSideOutput(new OutputTag<Tuple3<String, Long, Double>>("low-temp"));

        highTempStream.print("hight");
        lowTempStream.print("low");

        env.execute("side test");



    }
}

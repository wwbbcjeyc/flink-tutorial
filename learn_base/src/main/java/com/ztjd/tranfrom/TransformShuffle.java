package com.ztjd.tranfrom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformShuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> inputDS = env.readTextFile("/Users/wangwenbo/data/sensor-data.log");
        inputDS.print("input");

        DataStream<String> resultDS = inputDS.shuffle();
        resultDS.print("shuffle");

        env.execute();
    }
}

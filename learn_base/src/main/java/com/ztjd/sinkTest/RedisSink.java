package com.ztjd.sinkTest;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisSink {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

       DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) input -> {
            String[] dataArray = input.split(",");
            return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

       dataStream.addSink(new org.apache.flink.streaming.connectors.redis.RedisSink<SensorReading>(config,new MyRedisMapper()));

        env.execute("redis sink test job");

    }
}

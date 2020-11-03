package com.ztjd.sinkTest;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class FileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) input -> {
            String[] dataArray = input.split(",");
            return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
        });

         DataStreamSink<SensorReading> sensorReadingDataStreamSink = dataStream.addSink(
                StreamingFileSink.forRowFormat(new Path("/Users/wangwenbo/out.txt")
                        , new SimpleStringEncoder<SensorReading>("UTF-8"))
                        /*.withBucketAssigner(new PaulAssigner<>()) //分桶策略
                        .withRollingPolicy(new PaulRollingPolicy<>()) //滚动策略
                        .withBucketCheckInterval(CHECK_INTERVAL) //检查周期*/

                        .build());

         env.execute();
    }
}

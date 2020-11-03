package com.ztjd.processFun;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProcessFuntionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) input -> {
            String[] dataArray = input.split(",");
            return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
        });


        SingleOutputStreamOperator<String> warningStream = dataStream
                .keyBy("id")
                .process(new TempIncreWarning(5000L));


        warningStream.print();

        env.execute();


    }


}

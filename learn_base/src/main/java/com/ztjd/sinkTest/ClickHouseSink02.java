package com.ztjd.sinkTest;

import com.ztjd.bean.WaterSensorTable03;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author wangwenbo
 * @Date 2020/11/23 11:25 下午
 * @Version 1.0
 */
public class ClickHouseSink02 {

    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据、转换
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("127.0.0.1", 9999);
        stringDataStreamSource.print();


        env.execute("SinkClickHouse");
    }
}

package com.ztjd.windowTest;

import com.ztjd.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.naming.Context;

/**
 * @author wangwenbo
 * @version 1.0
 * @date 2020/10/19 2:13 下午
 */
public class TimeCharacteristicProcessingTime {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.env指定时间语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                });


        // 分组、开窗、聚合
        sensorDS
                .keyBy(data -> data.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        /**
                         * 全窗口函数：整个窗口的本组数据，存起来，关窗的时候一次性一起计算
                         */
                        new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                                out.collect(elements.spliterator().estimateSize());
                            }
                        })
                .print();


        env.execute();
    }
}

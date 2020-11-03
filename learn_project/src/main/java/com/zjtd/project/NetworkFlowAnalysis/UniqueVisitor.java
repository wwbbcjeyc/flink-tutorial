package com.zjtd.project.NetworkFlowAnalysis;

import com.zjtd.project.bean.UserBehavior;
import com.zjtd.project.bean.UvCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;

/**网站独立访客数（UV）的统计**/
public class UniqueVisitor {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_project/src/main/resources/UserBehavior.csv");

        final SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                final String[] dataArray = value.split(",");
                return new UserBehavior(Long.valueOf(dataArray[0]), Long.valueOf(dataArray[1]), Integer.valueOf(dataArray[2]), dataArray[3], Long.valueOf(dataArray[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.timestamp * 1000L;
            }
        });

         dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.behavior);
            }
        })
        .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult()).print();



        env.execute("uv job");
    }

    private static class UvCountResult implements AllWindowFunction<UserBehavior,UvCount, TimeWindow> {


        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> out) throws Exception {

            // 用一个集合来保存所有的userId，实现自动去重
             HashSet<Long> idSet = new HashSet<>();

             Iterator<UserBehavior> iterator = values.iterator();
            //遍历所有数据，添加到set中
            while (iterator.hasNext()) {
                UserBehavior userBehavior = iterator.next();
                long userId = userBehavior.getUserId();
                idSet.add(userId);
            }


            out.collect(UvCount.of(window.getEnd(),idSet.size()));
        }
    }
}

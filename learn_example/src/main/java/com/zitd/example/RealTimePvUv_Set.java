package com.zitd.example;

import com.zitd.example.bean.Result;
import com.zitd.example.utils.MySource1;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * 计算网站的uv
 * 1.实时计算出当天零点截止到当前时间各个端(android,ios,h5)下的uv
 * 2.每秒钟更新一次统计结果
 */

public class RealTimePvUv_Set {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.addSource(new MySource1());

        dataStreamSource.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new MyAggregate(),new WindowResult())
                .print();

        env.execute();
    }

    private static class MyAggregate implements AggregateFunction<Tuple2<String,Integer>, Set<Integer>,Integer> {
        @Override
        public Set<Integer> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Integer> add(Tuple2<String, Integer> value, Set<Integer> accumulator) {

            accumulator.add(value.f1);
            return accumulator;
        }

        @Override
        public Integer getResult(Set<Integer> accumulator) {
            return accumulator.size();
        }

        @Override
        public Set<Integer> merge(Set<Integer> a, Set<Integer> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class WindowResult implements WindowFunction<Integer, Result, Tuple, TimeWindow> {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(Tuple key, TimeWindow window,
                          Iterable<Integer> input, Collector<Result> out) throws Exception {
            String type = ((Tuple1<String>) key).f0;

            int uv = input.iterator().next();
            Result result = new Result();
            result.setType(type);
            result.setUv(uv);
            result.setDateTime(simpleDateFormat.format(new Date()));
            out.collect(result);


        }
    }
}

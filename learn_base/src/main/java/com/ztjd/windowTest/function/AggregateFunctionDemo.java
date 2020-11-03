package com.ztjd.windowTest.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class AggregateFunctionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建输入数据
        List<Tuple2<String, Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        data.add(a);
        data.add(b);
        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);

        // 自定义一个AggregateFunciton, 将相同标号 f1 的数据的 f0字符串字段合并在一起
        // ("hello", 1L) + ("world", 1L) = ("hello world", 1L)
        SingleOutputStreamOperator<String> aggregate = input.keyBy(x -> x.f1)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .aggregate(new MyAggregateFunction());

        aggregate.print();

        env.execute();
    }

    private static class MyAggregateFunction implements AggregateFunction<Tuple2<String,Long>,String,String> {
        @Override
        public String createAccumulator() {
            //初始化累加器
            return "";
        }

        @Override
        public String add(Tuple2<String, Long> value, String accumulator) {
            // 输入数据与累加器的合并
            return accumulator + " "+ value.f0;
        }

        @Override
        public String getResult(String accumulator) {
            // 得到累加器的结果
            return accumulator.trim();
        }

        @Override
        public String merge(String a, String b) {
            // 合并累加器
            return a+" "+b;
        }
    }
}

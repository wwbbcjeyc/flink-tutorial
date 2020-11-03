package com.ztjd.windowTest.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class ReduceFunctionDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建输入数据
        List<Tuple2<String, Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> a1 = new Tuple2<>("first event1", 1L);
        Tuple2<String, Long> a2 = new Tuple2<>("first event2", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        Tuple2<String, Long> b2 = new Tuple2<>("second event2", 2L);
        Tuple2<String, Long> b3 = new Tuple2<>("second event3", 2L);
        data.add(a);
        data.add(a1);
        data.add(a2);
        data.add(b);
        data.add(b2);
        data.add(b3);
        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);
        // 将Tuple2 按照 f1 进行 keyBy, 之后将 f0字符合并起来
//        input.keyBy(x -> x.f1).timeWindow(Time.seconds(10), Time.seconds(1))
//                .reduce((t1,t2) -> new Tuple2<>(t1.f0 + t2.f0, t1.f1));
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = input.keyBy(x -> x.f1)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return new Tuple2<>(t1.f0 + t2.f0, t1.f1);
                    }
                });

        reduce.print();
        env.execute();
    }

}

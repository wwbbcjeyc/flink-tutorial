package com.ztjd.windowTest.assigner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/**
 * 滚动窗口
 */
public class TumblingWindow {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 构建输入数据
        ArrayList<Tuple2<String,Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        data.add(a);
        data.add(b);


        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);

        input.print();
        // 使用 ProcessTime 滚动窗口, 10s 为一个窗口长度
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = input.keyBy(x -> x.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new MyWindowFunction());

        reduce.print();


        env.execute("TumblingWindow");

    }
}

package com.ztjd.wcTest;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class LambdaStreamWordCount {
    public static void main(String[] args) throws Exception {

        //1.创建一个 flink steam 程序的执行环境
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.获取 DataSource 数据来源
         DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3.Transformations 过程
        //Transformations 过程(注意此处：最后添加了 returns()方法，返回具体的类型，这样就不会报泛型类型参数丢失)
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                 Arrays.stream(line.split(" "))
                         .forEach(word -> out.collect(Tuple2.of(word, 1))))
                .returns(new TypeHint<Tuple2<String, Integer>>(){});

         SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = words.keyBy(0).sum(1);

         sumed.print();

         env.execute("LambdaStreamWordCount");
    }
}

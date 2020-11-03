package com.ztjd.wcTest;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //1.离线批处理使用的环境:ExecutionEnvironment

         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //使用 ExecutionEnvironment 创建 DataSet
        DataSet<String> lines = env.readTextFile(args[0]);


        //2.Transformations 操作
        //切分压平

        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(line.split(" ")).forEach(word -> out.collect(Tuple2.of(word, 1)));
        }).returns(new TypeHint<Tuple2<String, Integer>>() {});


        //聚合
        //离线计算实现的是分组聚合，调用的是groupBy()。实时计算调用的是 keyBy()
        AggregateOperator<Tuple2<String, Integer>> sumed = wordAndOne.groupBy(0).sum(1);

        //将结果保存成文本(或者保存到 hdfs 等)
        //setParallelism(1)：设置并行度
        sumed.writeAsText(args[1]).setParallelism(1);

        env.execute("BatchWordCount");
    }
}

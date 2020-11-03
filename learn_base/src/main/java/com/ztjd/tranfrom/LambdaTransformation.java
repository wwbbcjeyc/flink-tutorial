package com.ztjd.tranfrom;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LambdaTransformation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //map() 取出一个元素，根据规则处理后，并产生一个元素。可以用来做一些数据清洗的工作
       /* DataStreamSource<Integer> dataSource = env.fromElements(1, 2, 3, 4, 5);
        dataSource.map(num -> num *2).returns(Types.INT).print();*/

        //flatMap() 取出一个元素，并产生零个、一个或多个元素
         /*DataStreamSource<String> dataSource = env.fromElements("Hadoop Flink Storm HBase", "Spark Tomcat Spring MyBatis", "Sqoop Flume Docker K8S Scala");
         SingleOutputStreamOperator<String> streamOperator = dataSource.flatMap((String line, Collector<String> out) -> {
            Arrays.stream(line.split(" ")).forEach(str -> {
                if (str.startsWith("S")) {
                    out.collect(str);
                }
            });
        }).returns(Types.STRING);
        streamOperator.print();*/

        //filter() 为取出的每个元素进行规则判断(返回true/false)，并保留该函数返回 true 的数据。
        /*DataStreamSource<String> dataStreamSource = env.fromElements("Hadoop", "Spark", "Tomcat", "Storm", "Flink", "Docker", "Hive", "Sqoop" );
        dataStreamSource.filter(str -> str.startsWith("S")).returns(Types.STRING).print();*/

        //keyBy() 按 key 进行分组，key相同的(一定)会进入到同一个组中。具有相同键的所有记录都会分配给同一分区。在内部，keyBy() 是通过哈希分区实现的，有多种指定密钥的方法。
        //POJO类型：以 “属性名” 进行分组(属性名错误或不存在，会提示错误)
        /*DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<WordCount> streamOperator = lines.flatMap((String line, Collector<WordCount> out) -> {
            Arrays.stream(line.split(" ")).forEach(str -> out.collect(WordCount.of(str, 1)));
        }).returns(WordCount.class);
        streamOperator.keyBy("word").sum("count").print();*/

        //Tuple(元组)类型：以“0、1、2”等进行分组(角标从0开始)
        /*DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(line.split(" ")).forEach(str -> out.collect(Tuple2.of(str, 1)));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        streamOperator.keyBy(0).sum(1).print();*/

        //reduce() 如果需要将数据流中的所有数据，归纳得到一个数据的情况，可以使用 reduce() 方法。如果需要对数据流中的数据进行求和操作、求最大/最小值等(都是归纳为一个数据的情况)，此处就可以用到 reduce() 方法
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.map(str -> Tuple2.of(str, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);
        //对分组后的数据进行 reduce() 操作
        //old,news 为两个 Tuple2<String,Integer>类型(通过f0,f1可以获得相对应下标的值)
        keyedStream.reduce((old,news)->{
            old.f1 += news.f1;
            return old;
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).print();

        //fold() 一个有初始值的分组数据流的滚动折叠操作。合并当前元素和前一次折叠操作的结果，并产生一个新的值。
       /* DataStreamSource<Integer> source = env.fromElements(11, 11, 11, 22, 33, 44, 55);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> streamOperator = source.map(num ->Tuple2.of(num,1)).returns(Types.TUPLE(Types.INT,Types.INT));
        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);
        keyedStream.fold("start",(current, tuple) ->current + "-" + tuple.f0).returns(Types.STRING).print();*/

        // union() union 算子可以合并多个同类型的数据流，并生成同类型的新数据流，即可以将多个 DataStream 合并为一个新的 DataStream。数据将按照先进先出（First In First Out） 的模式合并，且不去重。
       /* DataStreamSource<String> streamSource01 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> streamSource02 = env.socketTextStream("localhost", 9999);
        DataStream<String> union = streamSource01.union(streamSource02);
        union.print();*/

        //connect() connect 提供了和 union 类似的功能，用来连接两个数据流，它与union的区别在于：
        //1.connect 只能连接两个数据流，union 可以连接多个数据流；
        //2.connect 所连接的两个数据流的数据类型可以不一致，union所连接的两个数据流的数据类型必须一致。
        //3.两个DataStream 经过 connect 之后被转化为 ConnectedStreams，ConnectedStreams 会对两个流的数据应用不同的处理方法，且双流之间可以共享状态。
        //demo 对两个不同类型的流合并，并计算流中出现单词/数字的次数
        DataStreamSource<String> streamSource01 = env.fromElements("aa", "bb", "aa", "dd", "dd");
        DataStreamSource<Integer> streamSource02 = env.fromElements(11,22,33,22,11);
        ConnectedStreams<String, Integer> connectedStream = streamSource01.connect(streamSource02);
        final SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = connectedStream.map(new CoMapFunction<String, Integer, Tuple2<String, Integer>>() {

            //处理第一个流数据
            @Override
            public Tuple2<String, Integer> map1(String str) throws Exception {
                return Tuple2.of(str, 1);
            }

            //处理第二个流数据
            @Override
            public Tuple2<String, Integer> map2(Integer num) throws Exception {
                return Tuple2.of(String.valueOf(num), 1);
            }
        });
        outputStreamOperator.keyBy(0).sum(1).print();


        //split()  按照指定标准，将指定的 DataStream流拆分成多个流，用SplitStream来表示
        //select() 从一个 SplitStream 流中，通过 .select()方法来获得想要的流
        DataStreamSource<Integer> streamSource = env.fromElements(1,2,3,4,5,6,7,8,9);
        final SplitStream<Integer> splitStream = streamSource.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even number");
                } else {
                    output.add("odd number");
                }
                return output;
            }
        });
        DataStream<Integer> bigData = splitStream.select("odd number");
        bigData.map(num -> Tuple2.of(num, 1)).returns(Types.TUPLE(Types.INT,Types.INT)).print();

        env.execute("Transformation");
    }
}

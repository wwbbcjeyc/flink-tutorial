package com.ztjd.tranfrom;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建数据流
        DataStreamSource<String> dataStreamSource = env.fromElements("Hadoop Flink Storm HBase", "Spark Tomcat Spring MyBatis", "Sqoop Flume Docker K8S Scala");
        //将Stream流中以 "S" 开头的数据，输出到 Collectior 集合中
        SingleOutputStreamOperator<String> streamOperator = dataStreamSource.flatMap((String line, Collector<String> out) -> {
            Arrays.stream(line.split(" ")).forEach(str ->{
                if (str.startsWith("S")) {
                    out.collect(str);
                }
            });
        }).returns(Types.STRING);

        streamOperator.print("www");

        env.execute("FlatMapDemo");
    }
}

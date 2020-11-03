package com.ztjd.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateBackEndDemo {
    // ./bin/flink run -c com.ztjd.state.StateBackEndDemo -p 2 /Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/target/learn_base-1.0-SNAPSHOT.jar
    // ./bin/flink run -c com.ztjd.state.StateBackEndDemo -s  file:/Users/wangwenbo/flink-1.11.0/checkpoint/7d02c14017abb5c51a19593bf7f1ccfe/chk-16  -p 2 /Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/target/learn_base-1.0-SNAPSHOT.jar
    // saveCheckpoint:file:/Users/wangwenbo/flink-1.11.0/checkpoint/7d02c14017abb5c51a19593bf7f1ccfe/chk-16
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //只有开启了checkpointing,才会有重启策略(多长时间执行一次checkpoint)
        env.enableCheckpointing(5000);

        //默认的重启策略是：固定延迟无限重启
        //设置重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                2));//重启3次，隔2秒一次

        //设置状态数据存储后端(此处设置后,会覆盖 flink-conf.yaml 中的配置)
        env.setStateBackend(new FsStateBackend("file:/Users/wangwenbo/flink-1.11.0/checkpoint"));

        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //通过Socket实时获取数据
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据转换成 Tuple 元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String str) throws Exception {
                return Tuple2.of(str, 1);
            }
        });

        //keyBy()
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);
        //sum()
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.sum(1);

        summed.print();

        env.execute("StateBackEndDemo");

    }
}

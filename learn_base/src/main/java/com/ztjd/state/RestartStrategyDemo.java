package com.ztjd.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 1. TODO Flink重启策略Demo
 2.  场景：
 3.  1.从 Socket 端口号中读取消息，并对消息中出现的 word 进行次数求和。
 4.  2.当 Socket 端口号中输入消息为 error时，则手动抛出 RuntimeException 异常，此时任务将会根据配置的重启策略开始重启。
 */

public class RestartStrategyDemo {

    public static void main(String[] args) throws Exception {
        /**1.创建流运行环境**/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**请注意此处：**/
        //1.只有开启了CheckPointing,才会有重启策略
        env.enableCheckpointing(5000);
        //2.默认的重启策略是：固定延迟无限重启
        //此处设置重启策略为：出现异常重启1次，隔5秒一次
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(5)));

        /**2.设置从Socket端口号中读取数据**/
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        /**3.Transformation过程**/
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.map(str -> {
            //如果输入"error",则抛出异常。任务将会根据重启策略开始重启
            if ("error".equals(str)) {
                throw new RuntimeException("程序执行异常");
            }
            return Tuple2.of(str, 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //对元组 Tuple2 分组求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamOperator.keyBy(0).sum(1);

        /**4.Sink过程**/
        sum.print();

        /**5.任务执行**/
        env.execute("RestartStrategyDemo");

    }
}

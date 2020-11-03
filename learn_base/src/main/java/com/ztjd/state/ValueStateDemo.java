package com.ztjd.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 同一个key，每接受3条数据，我们就打印输出总和
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> source =
                env.fromElements(Tuple2.of("A", 2), Tuple2.of("A", 3), Tuple2.of("A", 4),
                        Tuple2.of("B", 5), Tuple2.of("B", 7), Tuple2.of("B", 9), Tuple2.of("C", 1));


        source.keyBy(0)
                .flatMap(new ValueStateSum())
                .print();

        env.execute("ValueStateDemo");
    }
}

class ValueStateSum extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    //其实这就是一个属性
    // 来保存你想要保存进去的数据
    //我们这里第一个存出现次数 然后存总数  这里的泛形可以使用其他类型
    private ValueState<Tuple2<Integer, Integer>> valueStateSun;

    /**
     * 初始化描述器
     * 然后每次从运行环境中获取
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                "valueStateName",  // 状态的名字
                Types.TUPLE(Types.STRING, Types.INT)); // 指定状态存储的数据类型
        valueStateSun = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Integer> element,
                        Collector<Tuple2<String, Integer>> out) throws Exception {
        // 拿到当前的状态
        Tuple2<Integer, Integer> currentState = valueStateSun.value();

        // 第一次可能里面是空的
        if (currentState == null) {
            currentState = Tuple2.of(0, 0);
        }
        // 更新状态中的数量
        currentState.f0 += 1;
        System.out.println(currentState.toString());

        // 更新状态中的总值
        currentState.f1 += element.f1;
        System.out.println(currentState.toString());

        // 更新状态
        valueStateSun.update(currentState);

        if (currentState.f0 >= 3) {
            // 输出总数
            out.collect(Tuple2.of(element.f0, currentState.f1));
            // 清空状态值
            valueStateSun.clear();
        }
    }
}

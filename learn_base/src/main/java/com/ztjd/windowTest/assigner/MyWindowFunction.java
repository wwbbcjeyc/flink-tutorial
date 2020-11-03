package com.ztjd.windowTest.assigner;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyWindowFunction implements ReduceFunction<Tuple2<String,Long>> {
    @Override
    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
        return new Tuple2<>(value1.f0+value2.f0,value2.f1);
    }
}

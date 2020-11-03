package com.ztjd.tranfrom;

import com.ztjd.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRollingAgg {

    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .socketTextStream("localhost",9999 );

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        // 3.按照 id 分组
        KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = sensorDS
                .map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                        return new Tuple3<>(value.getId(), value.getTs(), value.getVc());
                    }
                })
                .keyBy( r -> r.f0);

        // TODO 滚动聚合算子：来一条，聚合一条，输出一次
//        sensorKS.sum(2).print("sum");
        sensorKS.max(2).print("max");
//        sensorKS.min(2).print("min");

        env.execute();
    }


}

package com.ztjd.tranfrom;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;


public class TranfromTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/src/main/resources/sensor.txt");

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] dataArray = value.split(",");
                return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
            }
        });

        SingleOutputStreamOperator<SensorReading> reduceStream = dataStream
                .keyBy((SensorReading sensor) -> sensor.getId())
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading curState, SensorReading newDate) throws Exception {
                        return new SensorReading(curState.getId(), newDate.getTimestamp(), Math.min(newDate.getTemperature(),curState.getTemperature()));
                    }
                });
        //reduceStream.print();

        SplitStream<SensorReading> splitStream = reduceStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                ArrayList<String> output = new ArrayList<>();
                if (sensorReading.getTemperature() > 30) {
                    output.add("highTemp");
                } else {
                    output.add("lowTemp");
                }
                return output;
            }
        });
        //splitStream.print("splitStream:");
        DataStream<SensorReading> highTemp = splitStream.select("highTemp");
        DataStream<SensorReading> lowTemp = splitStream.select("lowTemp");
        DataStream<SensorReading> allTempStream = splitStream.select("allTempStream");
       // lowTemp.print("lowTemp:");


        SingleOutputStreamOperator<Tuple2<String, Double>> waringStream = highTemp
                .map((SensorReading sensor) -> Tuple2.of(sensor.getId(), sensor.getTemperature()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
         //waringStream.print();

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = waringStream.connect(lowTemp);

        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "high temp waring").toString();
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return Tuple2.of(value.getId(), "healthy").toString();
            }
        });

        resultStream.print();

       DataStream<SensorReading> union = highTemp.union(lowTemp, allTempStream);
        union.print("union:");
        env.execute();
    }
}

package com.ztjd.tranfrom;

import com.ztjd.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class TransformSplit {

    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("/Users/wangwenbo/data/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });


        SplitStream<WaterSensor> splitSS = sensorDS.split(new OutputSelector<WaterSensor>() {
                                                              @Override
                                                              public Iterable<String> select(WaterSensor value) {
                                                                  if (value.getVc() < 50) {
                                                                      return Arrays.asList("normal");
                                                                  } else if (value.getVc() > 80) {
                                                                      return Arrays.asList("warn");
                                                                  } else {
                                                                      return Arrays.asList("alarm");
                                                                  }
                                                              }
                                                          }
        );
        splitSS.select("normal").print("normal");
        splitSS.select("warn").print("warn");

        env.execute();
    }

}

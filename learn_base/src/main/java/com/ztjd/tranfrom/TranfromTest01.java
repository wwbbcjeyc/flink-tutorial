package com.ztjd.tranfrom;

import com.ztjd.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TranfromTest01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputDS = env.readTextFile("/Users/wangwenbo/data/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyRichMapFunction());

        sensorDS.print();

        env.execute();
    }

    /**
     * 继承 RichMapFunction，指定输入的类型，返回的类型
     * 提供了 open()和 close() 生命周期管理方法
     * 能够获取 运行时上下文对象 =》 可以获取 状态、任务信息 等环境信息
     */
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(getRuntimeContext().getTaskName() + datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }

}

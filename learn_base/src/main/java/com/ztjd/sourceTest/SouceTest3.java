package com.ztjd.sourceTest;

import com.ztjd.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SouceTest3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySourceFunction());

        inputDS.print();

        env.execute();
    }

    private static class MySourceFunction implements SourceFunction<WaterSensor> {

        private boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            Random random = new Random();
            while (flag){
                ctx.collect(
                        new WaterSensor(
                                "sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40
                        )
                );
                Thread.sleep(2000L);
            }

        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }
}

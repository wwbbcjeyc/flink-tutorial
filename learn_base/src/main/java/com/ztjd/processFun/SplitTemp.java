package com.ztjd.processFun;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 自定义Process Function，实现具体的分流功能
public class SplitTemp extends ProcessFunction<SensorReading,SensorReading> {

    private double threshold;



    public SplitTemp(){}

    public SplitTemp(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            // 判断温度值，是否小于阈值，如果小于输出到侧输出流
        if(value.getTimestamp()<threshold){
            ctx.output(new OutputTag<Tuple3>("low-temp"),Tuple3.of(value.getId(),value.getTimestamp(),value.getTemperature()));
        }else {
            out.collect(value);
        }
    }






}

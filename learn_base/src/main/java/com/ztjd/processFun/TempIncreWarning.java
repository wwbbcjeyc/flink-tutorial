package com.ztjd.processFun;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 自定义 Process Function，用来定义定时器，检测温度连续上升
 */
public class TempIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

    private Long interval;
    private ValueState<Double> lastTempState;
    private ValueState<Long> curTimerState;

    public TempIncreWarning(){}

    public TempIncreWarning(long interval) {
            this.interval=interval;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        // 把上一次的温度值，保存成状态
        lastTempState =  getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        //把注册的定时器时间戳保存成状态，方便删除
        curTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cur-timer", Long.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        // 先把状态取出来
        Double lastTemp = lastTempState.value();
        Long curTimerTs = curTimerState.value();

        if (lastTemp == null || curTimerTs == null) {
            lastTemp = 0.0 ;
            curTimerTs = 0L;
        }

        lastTempState.update(value.getTemperature());

        // 判断：如果温度上升并且没有定时器，那么注册定时器
        if(value.getTimestamp()>lastTemp && curTimerTs ==0){
            long ts = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(ts);
            //保存ts到状态
            curTimerState.update(ts);
        }else if(value.getTimestamp() < lastTemp || lastTemp ==0.0){
            // 如果温度值下降，直接删除定时器
            ctx.timerService().deleteProcessingTimeTimer(curTimerTs);
            // 清空状态
            curTimerState.clear();
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发，报警
        out.collect("传感器:"+ctx.getCurrentKey()+"温度连续5秒上升");
        // timer状态清空
        curTimerState.clear();
    }
}

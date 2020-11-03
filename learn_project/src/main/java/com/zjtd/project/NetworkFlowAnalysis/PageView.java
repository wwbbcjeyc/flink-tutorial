package com.zjtd.project.NetworkFlowAnalysis;

import com.zjtd.project.bean.PvCount;
import com.zjtd.project.bean.UserBehavior;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 网站总浏览量（PV）的统计 **/
public class PageView {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv");

        final SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                final String[] dataArray = value.split(",");
                return new UserBehavior(Long.valueOf(dataArray[0]), Long.valueOf(dataArray[1]), Integer.valueOf(dataArray[2]), dataArray[3], Long.valueOf(dataArray[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 进行开窗统计聚合
        final SingleOutputStreamOperator<PvCount> pvCountStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        }).map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of(RandomStringUtils.randomAlphanumeric(4), 1L);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountWindowResult());
        //pvCountStream.print();

        // 把每个key对应的pv count值合并
        pvCountStream.keyBy("windowEnd")
                .process(new TotalPvCount())
                .print();
        env.execute("pv job");
    }

    // 自定义预聚合函数
    private static class PvCountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class PvCountWindowResult implements WindowFunction<Long, PvCount, String, TimeWindow> {


        @Override
        public void apply(String tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            out.collect(PvCount.of(window.getEnd(),input.iterator().next()));
        }
    }

    private static class TotalPvCount extends KeyedProcessFunction<Tuple,PvCount,PvCount>{

        // 定义一个状态，用来保存当前已有的key的count值总计
        private ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("total-count", Long.class));

        }

        @Override
        public void processElement(PvCount value, Context ctx, Collector<PvCount> out) throws Exception {
            Long currentTotalCount = totalCountState.value();
            if(currentTotalCount==null){
                totalCountState.update(1L);
            }else {
                totalCountState.update(currentTotalCount+value.count);
            }


            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PvCount> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发时，直接输出当前的totalcount
            Tuple currentKey = ctx.getCurrentKey();
            Long timeWindow = ((Tuple1<Long>) currentKey).f0;
            out.collect( PvCount.of(timeWindow, totalCountState.value()));
            // 清空状态
            totalCountState.clear();

        }
    }
}

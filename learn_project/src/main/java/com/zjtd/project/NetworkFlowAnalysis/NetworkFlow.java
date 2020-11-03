package com.zjtd.project.NetworkFlowAnalysis;

import com.zjtd.project.bean.ApacheLogEvent;
import com.zjtd.project.bean.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 */
public class NetworkFlow {

    private static final OutputTag<ApacheLogEvent> lateData = new OutputTag<ApacheLogEvent>("LATE") {
    };

    public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

         DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log");
         SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String value) throws Exception {
                final String[] dataArray = value.split(" ");

                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                long timestamp = sdf.parse(dataArray[3]).getTime();
                return new ApacheLogEvent(dataArray[0], dataArray[2], timestamp, dataArray[5], dataArray[6]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getEventTime();
            }
        });
         SingleOutputStreamOperator<PageViewCount> aggregate = dataStream
                 .filter(x -> "GET".equals(x.getMethod()))
                 .keyBy("url")
                 .timeWindow(Time.minutes(10), Time.seconds(5))
                 .allowedLateness(Time.minutes(1))
                 .sideOutputLateData(lateData)
                 .aggregate(new PageCountAgg(), new PageCountWindowResult());

         //aggregate.print();

         SingleOutputStreamOperator<String> reusltStream = aggregate.keyBy("windowEnd")
                .process(new TopNHotPages(3));

        reusltStream.print("result");
        reusltStream.getSideOutput(lateData).print("late:");


        env.execute();

    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1L;
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

    private static class PageCountWindowResult implements WindowFunction<Long, PageViewCount, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {

            String url = ((Tuple1<String>) key).f0;
            Long count = input.iterator().next();

            out.collect(new PageViewCount(url,window.getEnd(),count));
        }


    }

    private static class TopNHotPages extends KeyedProcessFunction<Tuple,PageViewCount,String> {

        private  int topSize;
        //private ListState<PageViewCount> listState;
        private MapState<String,Long> mapState;

        public TopNHotPages(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 定义列表状态，用来保存当前窗口的所有page的count值
           /* ListStateDescriptor<PageViewCount> listStatedescriptor = new ListStateDescriptor<>("pageview-count", PageViewCount.class);
            listState = getRuntimeContext().getListState(listStatedescriptor);*/

            // 改进：定义MapState，用来保存当前窗口所有page的count值，有更新操作时直接put
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("pageview-count", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);

        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {

            //listState.add(value);
            mapState.put(value.getUrl(),value.getCount());
            ctx.timerService().registerEventTimeTimer(value.windowEnd +1);
            // 定义1分钟之后的定时器，用于清除状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            Long windowEnd = ((Tuple1<Long>) ctx.getCurrentKey()).f0;
            // 判断时间戳，如果是1分钟后的定时器，直接清空状态
            if(timestamp ==windowEnd + 60 * 1000L){
                mapState.clear();
                return;
            }

            List<PageViewCount> pageViewCounts = new ArrayList<>();

            final Iterator<Map.Entry<String, Long>> iterator = mapState.entries().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, Long> next = iterator.next();
                pageViewCounts.add(new PageViewCount(next.getKey(),timestamp -1,next.getValue()));
            }


/*
            // 获取收到的url点击量
            Iterator<PageViewCount> iter = listState.get().iterator();
            while (iter.hasNext()) {
                pageViewCounts.add(iter.next());
            }

            // 清空状态
            listState.clear();*/

            //将所有count值排序取前 N 个
            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    return (int)(o2.count-o1.count);
                }
            });

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0; i<pageViewCounts.size() && i < topSize; i++) {
                PageViewCount pageViewCount = pageViewCounts.get(i);
                result.append("No").append(i).append(":")
                        .append("  页面url=").append(pageViewCount.getUrl())
                        .append("  访问量=").append(pageViewCount.getCount())
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());





        }
    }
}

package com.zjtd.project.HotItemsAnalysis;

import com.zjtd.project.bean.ItemViewCount;
import com.zjtd.project.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
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

import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 */
public class Hotltems {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

         URL file = Hotltems.class.getClassLoader().getResource("UserBehavior.csv");
         DataStreamSource<String> dataStreamSource = env.readTextFile(String.valueOf(file));

         SingleOutputStreamOperator<UserBehavior> dataStream = dataStreamSource
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] dataArray = value.split(",");
                        return new UserBehavior(Long.valueOf(dataArray[0]), Long.valueOf(dataArray[1]), Integer.valueOf(dataArray[2]), dataArray[3], Long.valueOf(dataArray[4]));
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

         SingleOutputStreamOperator<ItemViewCount> aggStream = dataStream.keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult());

         //aggStream.print();
        aggStream.keyBy("windowEnd")
                .process(new TopNHotItems(5)).print();


        env.execute();

    }

    private static class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator +1L;
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

    private static class WindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = ((Tuple1<Long>) tuple).f0;
            Long count = input.iterator().next();
            out.collect(ItemViewCount.of(itemId,window.getEnd(),count));
        }
    }


    private static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String> {

        // 定义一个列表状态，用来保存当前窗口的所有商品的count值
        private ListState<ItemViewCount> itemState;

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>("itemViewCount-liststate", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);

        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

            //每条都保存到状态中
            System.out.println(ctx.timestamp());

            // 需要注册一个windowEnd+1的定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd +1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemState.get()) {
                allItems.add(itemViewCount);
            }
            //提前清除数据
            itemState.clear();

            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间:").append(new Timestamp(timestamp-1)).append("\n");

            for (int i = 0; i < allItems.size() && i<topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                result.append("No").append(i).append(":")
                        .append(" 商品ID: ").append(currentItem.itemId)
                        .append(" 浏览量: ").append(currentItem.viewCount)
                        .append("\n");

            }
            result.append("====================================\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());

        }

    }
}

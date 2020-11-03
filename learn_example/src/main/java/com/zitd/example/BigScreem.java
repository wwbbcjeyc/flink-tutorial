package com.zitd.example;

import com.zitd.example.bean.CategoryPojo;
import com.zitd.example.utils.MySource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 模拟实现电商实时大屏显示
 * 1.实时计算出当天零点截止到当前时间的销售总额
 * 2.计算出各个分类的销售top3
 * 3.每秒钟更新一次统计结果
 */
public class BigScreem {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Double>> dataStream = env.addSource(new MySource());

        dataStream.print("step1:");
        final SingleOutputStreamOperator<CategoryPojo> result = dataStream.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new PriceAggregate(), new WindowResult());

        result.print("step2:");

        result.keyBy("dateTime")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(
                        1)))
                .process(new WindowResultProcess());

        env.execute();
    }

    private static class PriceAggregate implements AggregateFunction<Tuple2<String,Double>,Double,Double> {

        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return null;
        }
    }

    private static class WindowResult
            implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Double> input
                , Collector<CategoryPojo> out) throws Exception {

            CategoryPojo categoryPojo = new CategoryPojo();
            categoryPojo.setCategory(((Tuple1<String>) key).f0);
            BigDecimal bg = new BigDecimal(input.iterator().next());
            double p = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            categoryPojo.setTotalPrice(p);
            categoryPojo.setDateTime(simpleDateFormat.format(new Date()));
            out.collect(categoryPojo);

        }
    }


    private static class WindowResultProcess extends ProcessWindowFunction<CategoryPojo,Object,Tuple,TimeWindow> {




        @Override
        public void process(Tuple key, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {

            final String record_date = ((Tuple1<String>) key).f0;

            Queue<CategoryPojo> queue = new PriorityQueue<>(
                    3,
                    (o1, o2)->o1.getTotalPrice() >= o2.getTotalPrice() ? 1 : -1);

            double price = 0D;
            Iterator<CategoryPojo> iterator = elements.iterator();
            int s = 0;
            while (iterator.hasNext()) {
                CategoryPojo categoryPojo = iterator.next();
                //使用优先级队列计算出top3
                if(queue.size()<3){
                    queue.add(categoryPojo);
                }else{
                    //计算topN的时候需要小顶堆,也就是要去掉堆顶比较小的元素
                    CategoryPojo tmp = queue.peek();
                    if (categoryPojo.getTotalPrice() > tmp.getTotalPrice()){
                        queue.poll();
                        queue.add(categoryPojo);
                    }
                }
                price += categoryPojo.getTotalPrice();
            }

            //计算出来的queue是无序的，所以我们需要先sort一下
            List<String> list = queue.stream()
                    .sorted((o1, o2)->o1.getTotalPrice() <=
                            o2.getTotalPrice() ? 1 : -1)
                    .map(f->"(分类：" + f.getCategory() + " 销售额：" +
                            f.getTotalPrice() + ")")
                    .collect(
                            Collectors.toList());
            System.out.println("时间 ： " + record_date + "  总价 : " + price + " top3 " +
                    StringUtils.join(list, ","));
            System.out.println("-------------");

        }
    }
}

package com.zjtd.project.NetworkFlowAnalysis;

import com.zjtd.project.bean.UserBehavior;
import com.zjtd.project.bean.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

/**使用布隆过滤器的UV统计
 * 布隆过滤器是一种数据结构，比较巧妙的概率型数据结构（probabilistic data structure）
 * 特点是高效地插入和查询，可以用来告诉你 “某样东西一定不存在或者可能存在”。
 * 它本身是一个很长的二进制向量，既然是二进制的向量，那么显而易见的，存放的不是0，就是1。
 * 相比于传统的 List、Set、Map 等数据结构，它更高效、占用空间更少，但是缺点是其返回的结果是概率性的，而不是确切的。
 * 我们的目标就是:
 * 利用某种方法（一般是Hash函数）把每个数据，对应到一个位图的某一位上去；如果数据存在，那一位就是1，不存在则为0。
 * **/

public class UvWithBloom {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_project/src/main/resources/UserBehavior.csv");

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

        //dataStream.print();

        // 进行开窗统计聚合



        dataStream.filter(x-> "pv".equals(x.getBehavior()))
                .map(x -> Tuple2.of("uv",x.getUserId()))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                }))
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom()).print();

        env.execute("uv with bloom job");

    }

    // 自定义一个触发器
    private static class MyTrigger extends Trigger<Tuple2<String,Long>, TimeWindow> {
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
    }


    // 自定义处理逻辑，ProcessWindowFunction
    private static class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String,Long>, UvCount, Tuple,TimeWindow> {

        private Jedis jedis;
        private Bloom bloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jedis = new Jedis("localhost", 6379);
            bloomFilter = new Bloom(1L<<30L);
        }

        @Override
        public void process(Tuple key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UvCount> out) throws Exception {
            // 定义在redis中保存的位图的key，以当前窗口的end作为key，（windowEnd，bitmap）
            String storeKey = String.valueOf(context.window().getEnd()) ;
            // 把当前uv的count值也保存到redis中，保存成一张叫做count的hash表，（windowEnd，uvcount）
            String countMapKey = "count";

            // 初始化操作，从redis的count表中，查到当前窗口的uvcount值
           Long count = 0L;
           if(jedis.hget(countMapKey,storeKey) !=null){
               count = Long.valueOf(jedis.hget(countMapKey,storeKey)) ;
           }

            // 开始做去重，首先拿到userId
            Tuple2<String, Long> next = elements.iterator().next();
            String userId = String.valueOf(next.f1);

            // 调用布隆过滤器的hash函数，计算位图中的偏移量
            Long offset = bloomFilter.hash(userId, 61);

              // 使用redis命令，查询位图中对应位置是否为1
            Boolean isExist = jedis.getbit(storeKey, offset);

            if(!isExist){
                // 如果不存在userId，对应位图位置要置1，count加一
                jedis.setbit(storeKey,offset,true);
                jedis.hset(countMapKey,storeKey,String.valueOf(count + 1));
            }
        }

        /**
         *自定义一个布隆过滤器，位图是在外部redis，这里只保存位图的大小，以及hash函数
         **/
        private class Bloom implements Serializable {
            private Long cap ;

            public Bloom(Long cap) {
                this.cap=cap;
            }


            // 实现一个hash函数
            public Long hash(String value,int seed) {
                // 一般取cap是2的整次方 初始化大小
                Long result = 0L;
                for (int i = 0; i < value.length(); i++) {
                    // 用每个字符的ascii码值做叠加计算
                    result = result * seed + value.charAt(i);
                }
                // 返回一个cap范围内hash值
                return  (cap - 1) & result;
            }
        }



    }



}

package com.zitd.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/** 某活动下对应事件的pv uv **/
public class ActivityCountAndBloomFilter {

    public static void main(String[] args)throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //u001,A1,2019-09-02 10:10:11,1,北京市 数据
        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 8888);


        final SingleOutputStreamOperator<Tuple5<String, String, Long, String, String>> tpDataStream = lines.map(new MapFunction<String, Tuple5<String, String, Long, String, String>>() {

            @Override
            public Tuple5<String, String, Long, String, String> map(String lines) throws Exception {

                String[] spArr = lines.split(",");
                String uid = spArr[0];
                String act = spArr[1];
                Long dt = Long.valueOf(spArr[2]);
                String type = spArr[3];
                String province = spArr[4];

                return Tuple5.of(uid, act, dt, type, province);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<String, String, Long, String, String>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple5<String, String, Long, String, String> element) {
                return element.f2 * 1000L;
            }
        });



        KeyedStream<Tuple5<String, String, Long, String, String>, Tuple> keyedStream =
                tpDataStream.keyBy(1, 3);

        final SingleOutputStreamOperator<Tuple4<String, String, Long, Long>> result = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple5<String, String, Long, String, String>, Tuple4<String, String, Long, Long>>() {

            //保存分组数据去重后用户ID的布隆过滤器
            private transient ValueState<BloomFilter> bloomState = null;
            //保存去重后总人数的state，加transient禁止参与反序列化
            private transient ValueState<Long> uvCountState = null;
            //保存活动的点击数的state
            private transient ValueState<Long> pvCountState = null;

            private Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {

                jedis = new Jedis("localhost", 6379);

                ValueStateDescriptor<BloomFilter> bloomDescriptor = new ValueStateDescriptor<>(
                        "actId-type",
                        // 数据类型的class对象，因为是布隆过滤器，所有要使用这种方式来拿
                        TypeInformation.of(new TypeHint<BloomFilter>() {
                        })
                );
                ValueStateDescriptor<Long> uvCountDescriptor = new ValueStateDescriptor<>(
                        "uv-count",
                        Long.class
                );
                ValueStateDescriptor<Long> pvDescripor = new ValueStateDescriptor<>(
                        "pv-count",
                        Long.class
                );

                bloomState = getRuntimeContext().getState(bloomDescriptor);
                uvCountState = getRuntimeContext().getState(uvCountDescriptor);
                pvCountState = getRuntimeContext().getState(pvDescripor);
            }

            @Override
            public void processElement(Tuple5<String, String, Long, String, String> input, Context ctx, Collector<Tuple4<String, String, Long, Long>> out) throws Exception {
                String uid = input.f0;
                String actId = input.f1;
                String type = input.f3;

                //将值取出来，因为ValueState中实质上是以特殊的map集合存储的，一个key,一个value
                BloomFilter bloomFilter = bloomState.value();
                Long uvCount = uvCountState.value();
                Long pvCount = pvCountState.value();

                //初始化上面三个变量
                if (pvCount == null) {
                    pvCount = 0L;
                }
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    uvCount = 0L;
                }
                //布隆过滤器中是否不包含uid,是的话就返回false
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid); //不包含就添加进去
                    uvCount += 1;
                }

                pvCount += 1;
                bloomState.update(bloomFilter);
                uvCountState.update(uvCount);
                pvCountState.update(pvCount);

                out.collect(Tuple4.of(actId, type, pvCount, uvCount));
            }
        });


        result.print();
        env.execute("ActivityCountAndBloomFilter");
    }
}




